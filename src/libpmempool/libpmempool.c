/*
 * Copyright (c) 2016, Intel Corporation
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
 *     * Neither the name of Intel Corporation nor the names of its
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
 * libpmempool.c -- pmem entry points for libpmempool
 */

#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>

#include "libpmempool.h"

#include "pmempool.h"
#include "util.h"
#include "out.h"

/*
 * libpmempool_init -- load-time initialization for libpmempool
 *
 * Called automatically by the run-time loader.
 */
__attribute__((constructor))
static void
libpmempool_init(void)
{
	out_init(PMEMPOOL_LOG_PREFIX, PMEMPOOL_LOG_LEVEL_VAR, PMEMPOOL_LOG_FILE_VAR,
			PMEMPOOL_MAJOR_VERSION, PMEMPOOL_MINOR_VERSION);
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
 * pmempool_check_version -- see if library meets application version requirements
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
