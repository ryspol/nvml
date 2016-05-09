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

#include <stdlib.h>
#include <stdint.h>
#include <errno.h>

#include "out.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"

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
 * pmempool_ppc_set_default -- set default values of check context
 */
static void
pmempool_ppc_set_default(PMEMpoolcheck *ppc)
{
	const PMEMpoolcheck ppc_default = {
		.result	= PMEMPOOL_CHECK_RESULT_CONSISTENT,
	};
	*ppc = ppc_default;
}

/*
 * pmempool_check_init -- initialize check context according to passed
 *	arguments and prepare to perform a check
 */
PMEMpoolcheck *
pmempool_check_init(struct pmempool_check_args *args)
{
	ASSERTne(args, NULL);

	if (args->path == NULL) {
		ERR("path can not be NULL");
		errno = EINVAL;
		return NULL;
	}

	/*
	 * dry run does not allow to made changes possibly performed during
	 * repair. aggresive allow to perform more complex repairs.
	 * so dry run and aggresive can be set only if repair is set
	 */
	if (!args->repair && (args->dry_run || args->aggresive)) {
		ERR("dry run and aggresive is applicable only if repair is "
			"set");
		errno = EINVAL;
		return NULL;
	}

	/*
	 * dry run does not modify anything so performing backup is redundant
	 */
	if (args->dry_run && args->backup_path != NULL) {
		ERR("dry run does not allow to perform backup");
		errno = EINVAL;
		return NULL;
	}

	/*
	 * libpmempool uses str format of communication so it must be set
	 */
	if (!(args->flags & PMEMPOOL_CHECK_FORMAT_STR)) {
		ERR("PMEMPOOL_CHECK_FORMAT_STR flag must be set");
		errno = EINVAL;
		return NULL;
	}

	PMEMpoolcheck *ppc = malloc(sizeof (*ppc));
	if (ppc == NULL) {
		ERR("!malloc");
		return NULL;
	}
	pmempool_ppc_set_default(ppc);
	memcpy(&ppc->args, args, sizeof (ppc->args));
	ppc->path = strdup(args->path);
	if (!ppc->args.path) {
		ERR("!strdup");
		goto error_path_malloc;
	}
	ppc->args.path = ppc->path;
	if (args->backup_path != NULL) {
		ppc->backup_path = strdup(args->backup_path);
		if (!ppc->args.backup_path) {
			ERR("!strdup");
			goto error_backup_path_malloc;
		}
		ppc->args.backup_path = ppc->backup_path;
	}

	if (check_init(ppc) != 0)
		goto error_check_init;

	return ppc;

error_check_init:
	free(ppc->backup_path);
error_backup_path_malloc:
	free(ppc->path);
error_path_malloc:
	free(ppc);
	return NULL;
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

		if (check_ended(ppc->data) && result == NULL)
			return NULL;
	} while (result == NULL);

	return check_status_get(result);
}

/*
 * pmempool_check_end -- end check and release check context
 */
enum pmempool_check_result
pmempool_check_end(PMEMpoolcheck *ppc, struct pmempool_check_status *stat)
{
	enum pmempool_check_result result = ppc->result;

	check_fini(ppc);
	free(ppc->path);
	free(ppc->backup_path);
	free(ppc);

	return result;
}
