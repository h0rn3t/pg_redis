#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xact.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

#include <signal.h>

#include "bgworker.h"
#include "jobs.h"
#include "types.h"
#include "shmem.h"
#include "persistence.h"
#include "dirty_ring.h"
#include "utils/snapmgr.h"

pg_noreturn PGDLLEXPORT void pg_redis_bgworker_main(Datum main_arg);

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static void
pg_redis_bgworker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc != NULL)
		SetLatch(MyLatch);
	errno = save_errno;
}

static void
pg_redis_bgworker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc != NULL)
		SetLatch(MyLatch);
	errno = save_errno;
}

void
pg_redis_bgworker_register(void)
{
	BackgroundWorker w;

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(LOG,
				(errmsg("pg_redis: background worker not registered"),
				 errhint("Set shared_preload_libraries = 'pg_redis' and restart to enable.")));
		return;
	}

	if (!pg_redis_enable_bgworker)
	{
		ereport(LOG,
				(errmsg("pg_redis: background worker disabled via pg_redis.enable_background_worker")));
		return;
	}

	MemSet(&w, 0, sizeof(w));
	w.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	w.bgw_start_time = BgWorkerStart_RecoveryFinished;
	w.bgw_restart_time = 10;
	snprintf(w.bgw_library_name, BGW_MAXLEN, "pg_redis");
	snprintf(w.bgw_function_name, BGW_MAXLEN, "pg_redis_bgworker_main");
	snprintf(w.bgw_name, BGW_MAXLEN, "pg_redis bgworker");
	snprintf(w.bgw_type, BGW_MAXLEN, "pg_redis");
	w.bgw_notify_pid = 0;
	w.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&w);
}

void
pg_redis_bgworker_main(Datum main_arg)
{
	pqsignal(SIGTERM, pg_redis_bgworker_sigterm);
	pqsignal(SIGHUP, pg_redis_bgworker_sighup);
	BackgroundWorkerUnblockSignals();

	/* Connect to a database so SPI works. We use "postgres" by convention
	 * and document that operators should grant pg_redis access there. */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/* Publish our latch into shared memory so producers can wake us when
	 * the dirty-ring fills up. */
	pg_redis_shmem_set_bgw_latch(MyLatch);

	ereport(LOG,
			(errmsg("pg_redis bgworker started")));

	{
		TimestampTz last_drain = GetCurrentTimestamp();

		while (!got_sigterm)
		{
			int			rc;
			int			tick_seconds;
			uint64		pending;
			TimestampTz now;
			bool		drain_due;

			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();

			if (got_sighup)
			{
				got_sighup = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			/* 1. Job scheduler tick (existing behavior). */
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			PG_TRY();
			{
				(void) pg_redis_jobs_run_due();
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
				PopActiveSnapshot();
				AbortCurrentTransaction();
				StartTransactionCommand();
				PushActiveSnapshot(GetTransactionSnapshot());
			}
			PG_END_TRY();
			PopActiveSnapshot();
			CommitTransactionCommand();

			/* 2. Dirty-ring drain. Watermark = ring_size/4; periodic
			 * fallback = flush_interval. Either trigger drains a batch. */
			pending = pg_redis_dirty_ring_pending();
			now = GetCurrentTimestamp();
			drain_due = false;
			if ((int64) pending >= pg_redis_dirty_ring_size / 4)
				drain_due = true;
			else if (pending > 0 &&
					 TimestampDifferenceExceeds(last_drain, now,
												((int64) pg_redis_flush_interval_s) * 1000))
				drain_due = true;

			if (drain_due)
			{
				StartTransactionCommand();
				PG_TRY();
				{
					(void) pg_redis_persistence_async_drain(0);
				}
				PG_CATCH();
				{
					EmitErrorReport();
					FlushErrorState();
					AbortCurrentTransaction();
					StartTransactionCommand();
				}
				PG_END_TRY();
				CommitTransactionCommand();
				last_drain = GetCurrentTimestamp();
			}

			tick_seconds = pg_redis_flush_interval_s;
			if (tick_seconds <= 0)
				tick_seconds = 5;

			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   ((long) tick_seconds) * 1000L,
						   PG_WAIT_EXTENSION);
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);
		}
	}

	/* SIGTERM: final drain so already-acked events make it to disk. */
	if (pg_redis_dirty_ring_pending() > 0)
	{
		StartTransactionCommand();
		PG_TRY();
		{
			(void) pg_redis_persistence_async_drain(0);
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();
			AbortCurrentTransaction();
			StartTransactionCommand();
		}
		PG_END_TRY();
		CommitTransactionCommand();
	}

	pg_redis_shmem_set_bgw_latch(NULL);
	proc_exit(0);
}
