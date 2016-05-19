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
 * libpmempool_repl -- Placeholder for testing libpmempool replica.
 *
 */

#include <stddef.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "unittest.h"

enum oper_type
{
	LIBPMEMPOOL_SYNC,
	LIBPMEMPOOL_CONVERT
};

/*
 * print_code -- print message referenced to error code
 */
static const inline char*
print_code(enum pmempool_replica_result c)
{
	static const char *msg[] = {
		"REPLICA_RES_INTERNAL_ERR",
		"REPLICA_RES_WRONG_ARG",
		"REPLICA_RES_WRONG_REPL_NUM",
		"REPLICA_RES_PART_FILE_DEL_ERR",
		"REPLICA_RES_PART_FILE_CREATE_ERR",
		"REPLICA_RES_REP_CREATE_ERR",
		"REPLICA_RES_REP_OPEN_ERR",
		"REPLICA_RES_INSUF_TARGET_MEM",
		"REPLICA_RES_CANNOT_UUIDS_UPDATE",
		"REPLICA_RES_COPY_SUCCESSFUL",
		"REPLICA_RES_IN_POOLSET_ERR",
		"REPLICA_RES_OUT_POOLSET_ERR",
		"REPLICA_RES_CONVERT_ERR",
		"REPLICA_RES_CONVERT_OK",
		"REPLICA_RES_OK"
	};
	return msg[c];
}

/*
 * print_usage -- print usage of program
 */
static void
print_usage(char *name)
{
	UT_OUT("Usage: %s [-s] [-c] [-w <replica_to>]"
			"[-g <replica_from>] [-f <flags>]"
			"[-i <poolset_conv_path>] <poolset_path>\n", name);
}

int
main(int argc, char *argv[])
{
	enum pmempool_replica_result res;
	START(argc, argv, "libpmempool_repl");
	int opt;

	char *pool_set = NULL;
	char *path_conv_poolset = NULL;
	enum oper_type otype = LIBPMEMPOOL_SYNC;
	unsigned rep_to = 0;
	unsigned rep_from = 0;
	unsigned flags = 0;

	while ((opt = getopt(argc, argv, "scw:g:f:i:")) != -1) {
		switch (opt) {
		case 's':
			otype = LIBPMEMPOOL_SYNC;
			break;
		case 'c':
			otype = LIBPMEMPOOL_CONVERT;
			break;
		case 'w':
			rep_to = (unsigned)strtoul(optarg, NULL, 0);
			break;
		case 'g':
			rep_from = (unsigned)strtoul(optarg, NULL, 0);
			break;
		case 'f':
			flags = (unsigned)strtoul(optarg, NULL, 0);
			break;
		case 'i':
			path_conv_poolset = malloc(strlen(optarg));
			strcpy(path_conv_poolset, optarg);
			break;
		default:
			print_usage(argv[0]);
			return -1;
		}
	}
	if (optind < argc)
		pool_set = argv[optind];
	else
		print_usage(argv[0]);

	if (otype == LIBPMEMPOOL_CONVERT && path_conv_poolset == NULL) {
		UT_OUT("Option -c requires [-i <poolset_conv_path>]\n");
		return -1;
	}

	if (otype == LIBPMEMPOOL_SYNC && rep_to == 0 && rep_from ==0) {
		UT_OUT("Default option -s requires [-w <replica_to>] and "
			"[-g <replica_from>]\n");
		return -1;
	}

	if (otype == LIBPMEMPOOL_CONVERT)
		res = pmempool_convert(pool_set, path_conv_poolset, flags);
	else
		res = pmempool_sync(pool_set, rep_to, rep_from, flags);

	UT_OUT("Result: %s\n", print_code(res));

	free(path_conv_poolset);
	path_conv_poolset = NULL;
	DONE(NULL);
}
