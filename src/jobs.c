#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include <string.h>
#include <sys/time.h>

#include "jobs.h"
#include "persistence.h"
#include "ttl.h"

static bool dispatch_job(const char *job_type, char **err_out);
static void update_job_row(int64 job_id, const char *last_error,
						   int64 duration_ms, bool success);

static bool
dispatch_job(const char *job_type, char **err_out)
{
	if (err_out)
		*err_out = NULL;

	if (job_type == NULL)
	{
		if (err_out)
			*err_out = pstrdup("missing job_type");
		return false;
	}

	if (strcmp(job_type, "ttl_cleanup") == 0)
	{
		(void) pg_redis_ttl_sweep();
		return true;
	}
	if (strcmp(job_type, "snapshot_save") == 0)
	{
		(void) pg_redis_persistence_save_snapshot();
		return true;
	}
	if (strcmp(job_type, "flush_dirty_keys") == 0)
	{
		(void) pg_redis_persistence_flush_dirty_batch(1000);
		elog(DEBUG1, "pg_redis: flush_dirty_keys in v0.1 is a no-op outside shared mode");
		return true;
	}
	if (strcmp(job_type, "aof_rewrite") == 0)
	{
		if (err_out)
			*err_out = pstrdup("aof_rewrite is not implemented in v0.1");
		return false;
	}

	if (err_out)
		*err_out = psprintf("unknown job_type \"%s\"", job_type);
	return false;
}

static void
update_job_row(int64 job_id, const char *last_error, int64 duration_ms, bool success)
{
	{
		/* Update jobs.last_run / next_run / last_error. */
		Oid			argtypes[3] = {INT8OID, TEXTOID, BOOLOID};
		Datum		values[3];
		char		nulls[3] = {' ', ' ', ' '};
		const char *sql_jobs =
			"UPDATE pgredis.jobs "
			"   SET last_run = now(), "
			"       next_run = now() + schedule_interval, "
			"       last_error = CASE WHEN $3 THEN NULL ELSE $2 END "
			" WHERE job_id = $1";

		values[0] = Int64GetDatum(job_id);
		if (last_error != NULL)
			values[1] = CStringGetTextDatum(last_error);
		else
		{
			values[1] = (Datum) 0;
			nulls[1] = 'n';
		}
		values[2] = BoolGetDatum(success);

		(void) SPI_execute_with_args(sql_jobs, 3, argtypes, values, nulls, false, 0);

		if (last_error != NULL)
			pfree(DatumGetPointer(values[1]));
	}

	{
		/* Upsert job_stats with counter increments. */
		Oid			argtypes[4] = {INT8OID, BOOLOID, TEXTOID, INT8OID};
		Datum		values[4];
		char		nulls[4] = {' ', ' ', ' ', ' '};
		const char *sql_stats =
			"INSERT INTO pgredis.job_stats AS s "
			"  (job_id, total_runs, successful_runs, failed_runs, "
			"   last_duration_ms, last_success, last_failure, last_error) "
			"VALUES ($1, 1, "
			"        CASE WHEN $2 THEN 1 ELSE 0 END, "
			"        CASE WHEN $2 THEN 0 ELSE 1 END, "
			"        $4, "
			"        CASE WHEN $2 THEN now() ELSE NULL END, "
			"        CASE WHEN $2 THEN NULL ELSE now() END, "
			"        CASE WHEN $2 THEN NULL ELSE $3 END) "
			"ON CONFLICT (job_id) DO UPDATE SET "
			"  total_runs = s.total_runs + 1, "
			"  successful_runs = s.successful_runs + CASE WHEN $2 THEN 1 ELSE 0 END, "
			"  failed_runs    = s.failed_runs    + CASE WHEN $2 THEN 0 ELSE 1 END, "
			"  last_duration_ms = $4, "
			"  last_success = CASE WHEN $2 THEN now() ELSE s.last_success END, "
			"  last_failure = CASE WHEN $2 THEN s.last_failure ELSE now() END, "
			"  last_error   = CASE WHEN $2 THEN s.last_error ELSE $3 END";

		values[0] = Int64GetDatum(job_id);
		values[1] = BoolGetDatum(success);
		if (last_error != NULL)
			values[2] = CStringGetTextDatum(last_error);
		else
		{
			values[2] = (Datum) 0;
			nulls[2] = 'n';
		}
		values[3] = Int64GetDatum(duration_ms);

		(void) SPI_execute_with_args(sql_stats, 4, argtypes, values, nulls, false, 0);

		if (last_error != NULL)
			pfree(DatumGetPointer(values[2]));
	}
}

bool
pg_redis_jobs_run_one(int64 job_id)
{
	Oid			argtypes[1] = {INT8OID};
	Datum		values[1];
	char		nulls[1] = {' '};
	const char *sql_lookup =
		"SELECT job_type FROM pgredis.jobs "
		" WHERE job_id = $1 AND enabled";
	char	   *job_type = NULL;
	MemoryContext outer = CurrentMemoryContext;
	struct timeval t_begin,
				t_end;
	int64		duration_ms = 0;
	bool		success = false;
	char	   *err_msg = NULL;
	ResourceOwner saved_resowner;
	MemoryContext saved_cxt;

	if (SPI_connect() != SPI_OK_CONNECT)
		return false;

	values[0] = Int64GetDatum(job_id);
	if (SPI_execute_with_args(sql_lookup, 1, argtypes, values, nulls, true, 0) != SPI_OK_SELECT ||
		SPI_processed != 1)
	{
		SPI_finish();
		return false;
	}
	job_type = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	if (job_type != NULL)
		job_type = MemoryContextStrdup(outer, job_type);

	/* Run handler inside a subtransaction so handler errors don't abort the
	 * surrounding transaction. We still want to update jobs/job_stats after. */
	gettimeofday(&t_begin, NULL);

	saved_cxt = CurrentMemoryContext;
	saved_resowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		success = dispatch_job(job_type, &err_msg);
		if (success)
			ReleaseCurrentSubTransaction();
		else
			RollbackAndReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		MemoryContextSwitchTo(saved_cxt);
		ed = CopyErrorData();
		FlushErrorState();
		err_msg = MemoryContextStrdup(outer, ed->message ? ed->message : "unknown error");
		FreeErrorData(ed);
		RollbackAndReleaseCurrentSubTransaction();
		success = false;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(saved_cxt);
	CurrentResourceOwner = saved_resowner;

	gettimeofday(&t_end, NULL);
	duration_ms = ((int64) t_end.tv_sec - (int64) t_begin.tv_sec) * 1000 +
		((int64) t_end.tv_usec - (int64) t_begin.tv_usec) / 1000;
	if (duration_ms < 0)
		duration_ms = 0;

	update_job_row(job_id, success ? NULL : (err_msg ? err_msg : "unknown error"),
				   duration_ms, success);

	if (err_msg != NULL)
		pfree(err_msg);
	if (job_type != NULL)
		pfree(job_type);

	SPI_finish();
	return success;
}

int
pg_redis_jobs_run_due(void)
{
	const char *sql =
		"SELECT job_id FROM pgredis.jobs "
		" WHERE enabled AND next_run IS NOT NULL AND next_run <= now() "
		" ORDER BY next_run ASC";
	int			ran = 0;
	int64	   *ids = NULL;
	uint64		n = 0;
	MemoryContext outer = CurrentMemoryContext;

	if (SPI_connect() != SPI_OK_CONNECT)
		return 0;

	if (SPI_execute(sql, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
	{
		MemoryContext oldcxt;

		n = SPI_processed;
		oldcxt = MemoryContextSwitchTo(outer);
		ids = (int64 *) palloc(sizeof(int64) * n);
		MemoryContextSwitchTo(oldcxt);

		for (uint64 i = 0; i < n; i++)
		{
			bool		isnull;
			Datum		d = SPI_getbinval(SPI_tuptable->vals[i],
										  SPI_tuptable->tupdesc, 1, &isnull);

			ids[i] = isnull ? 0 : DatumGetInt64(d);
		}
	}
	SPI_finish();

	for (uint64 i = 0; i < n; i++)
	{
		if (ids[i] == 0)
			continue;
		if (pg_redis_jobs_run_one(ids[i]))
			ran++;
	}
	if (ids != NULL)
		pfree(ids);
	return ran;
}
