/*
 * Copyright 2016, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * libpmempool.c -- entry points for libpmempool
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/queue.h>

#include "util.h"
#include "out.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"

/*
 * pmempool_check_default -- default values of check context
 */
static const PMEMpoolcheck pmempool_check_default = {
	.result	= PMEMPOOL_CHECK_RESULT_CONSISTENT,
};

/*
 * libpmempool_init -- load-time initialization for libpmempool
 *
 * Called automatically by the run-time loader.
 */
__attribute__((constructor))
static void
libpmempool_init(void)
{
	out_init(PMEMPOOL_LOG_PREFIX, PMEMPOOL_LOG_LEVEL_VAR,
		PMEMPOOL_LOG_FILE_VAR, PMEMPOOL_MAJOR_VERSION,
		PMEMPOOL_MINOR_VERSION);
	LOG(3, NULL);
	util_init();
}

/*
 * libpmempool_fini -- libpmempool cleanup routine
 *
 * Called automatically when the process terminates.
 */
__attribute__((destructor))
static void
libpmempool_fini(void)
{
	LOG(3, NULL);
	out_fini();
}

/*
 * pmempool_check_version -- see if library meets application version
 *	requirements
 */
const char *
pmempool_check_version(unsigned major_required, unsigned minor_required)
{
	LOG(3, "major_required %u minor_required %u",
			major_required, minor_required);

	if (major_required != PMEMPOOL_MAJOR_VERSION) {
		ERR("libpmempool major version mismatch (need %u, found %u)",
			major_required, PMEMPOOL_MAJOR_VERSION);
		return out_get_errormsg();
	}

	if (minor_required > PMEMPOOL_MINOR_VERSION) {
		ERR("libpmempool minor version mismatch (need %u, found %u)",
			minor_required, PMEMPOOL_MINOR_VERSION);
		return out_get_errormsg();
	}

	return NULL;
}

/*
 * pmempool_errormsg -- return last error message
 */
const char *
pmempool_errormsg(void)
{
	return out_get_errormsg();
}

/*
 * pmempool_check_init -- initialize check context according to passed
 *	arguments and prepare to perform a check
 */
PMEMpoolcheck *
pmempool_check_init(struct pmempool_check_args *args)
{
	if (args->path == NULL) {
		errno = EINVAL;
		return NULL;
	}

	if (!args->repair && !args->dry_run) {
		errno = EINVAL;
		return NULL;
	}

	if (!args->repair && args->backup) {
		errno = EINVAL;
		return NULL;
	}

	if (args->repair && args->backup && !args->dry_run &&
		args->backup_path == NULL) {
		errno = EINVAL;
		return NULL;
	}

	if (!(args->flags & PMEMPOOL_CHECK_FORMAT_STR) &&
		!(args->flags & PMEMPOOL_CHECK_FORMAT_DATA)) {
		errno = EINVAL;
		return NULL;
	}

	PMEMpoolcheck *ppc = malloc(sizeof (*ppc));
	*ppc = pmempool_check_default;
	ppc->path = strdup(args->path);
	ppc->pool_type = args->pool_type;
	ppc->repair = args->repair;
	ppc->backup = args->backup;
	ppc->dry_run = args->dry_run;
	ppc->always_yes = args->always_yes;
	ppc->flags = args->flags;
	if (args->backup_path != NULL)
		ppc->backup_path = strdup(args->backup_path);

	if (check_start(ppc) != 0) {
		free(ppc->backup_path);
		free(ppc->path);
		free(ppc);
		return NULL;
	}

	return ppc;
}

/*
 * pmempool_check -- continue check till produce status to consume for caller
 */
struct pmempool_check_status *
pmempool_check(PMEMpoolcheck *ppc)
{
	struct check_status *result = NULL;
	do {
		result = check_step(ppc);

		if (ppc->data->step == PMEMPOOL_CHECK_END &&
			result == NULL)
			return NULL;
	} while (result == NULL);

	return &result->status;
}

/*
 * pmempool_check_end -- end check and release check context
 */
enum pmempool_check_result
pmempool_check_end(PMEMpoolcheck *ppc, struct pmempool_check_status *stat)
{
	enum pmempool_check_result result = ppc->result;

	check_stop(ppc);
	free(ppc->path);
	free(ppc->backup_path);
	free(ppc);

	return result;
}
