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
	/* BGW_NEVER_RESTART: if the configured pg_redis.bgworker_database does not
	 * exist, BackgroundWorkerInitializeConnection raises FATAL. With a finite
	 * restart time the postmaster would relaunch it on a loop, the dirty-ring
	 * would fill, and every producer would error. Never auto-restarting breaks
	 * that loop; operators fix the GUC and restart the cluster. (Decision 8.) */
	w.bgw_restart_time = BGW_NEVER_RESTART;
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

	/* Connect to a database so SPI works. The target is configurable via
	 * pg_redis.bgworker_database (default "postgres"). If that database does
	 * not exist, BackgroundWorkerInitializeConnection raises FATAL and — because
	 * we registered with BGW_NEVER_RESTART — the worker exits without looping.
	 * Log the target + GUC first so a missing-database FATAL is diagnosable. */
	{
		const char *dbname = (pg_redis_bgworker_database != NULL &&
							   pg_redis_bgworker_database[0] != '\0')
			? pg_redis_bgworker_database : "postgres";

		ereport(LOG,
				(errmsg("pg_redis bgworker connecting to database \"%s\"", dbname),
				 errdetail("Target database is configured via the "
						   "pg_redis.bgworker_database GUC. If it does not exist "
						   "the worker exits and does not restart.")));
		BackgroundWorkerInitializeConnection(dbname, NULL, 0);
	}

	/* Attach to the shared DSA segment before the drain loop: the drain frees
	 * DSA spill payloads via pg_redis_shared_pfree, which asserts the segment
	 * is attached. Done here (no pg_redis LWLock held) per the eager-attach
	 * contract. */
	pg_redis_shmem_attach_dsa();

	/* Publish our latch into shared memory so producers can wake us when
	 * the dirty-ring fills up. */
	pg_redis_shmem_set_bgw_latch(MyLatch);

	ereport(LOG,
			(errmsg("pg_redis bgworker started")));

	{
		TimestampTz last_drain = GetCurrentTimestamp();
		uint64		reclaim_tick = 0;

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

			/* Periodic reclamation pass: reset dirty-ring slots whose producer
			 * ereport'd between reserving and publishing (stuck in WRITING),
			 * unblocking the ring. Pure shmem work — no transaction needed. */
			if (pg_redis_ring_reclaim_tick_interval > 0 &&
				++reclaim_tick % (uint64) pg_redis_ring_reclaim_tick_interval == 0)
				(void) pg_redis_dirty_ring_reclaim_stuck();

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
