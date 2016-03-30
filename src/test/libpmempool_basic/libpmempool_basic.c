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
 * libpmempool_basic.c -- Placeholder for possible feature libpmempool tests.
 *	Currently used for basic testing.
 *
 */

#include <stddef.h>

#include "unittest.h"

#define	POOL_FILE		"test_file.pool"
#define	POOL_FILE_BACKUP	"test_file_backup.pool"

POBJ_LAYOUT_BEGIN(layout);
POBJ_LAYOUT_ROOT(layout, struct test);
POBJ_LAYOUT_END(layout);

struct test {
	int a;
};

/*static void
obj_prepare(void)
{
	unlink(POOL_FILE);
	PMEMobjpool *pool = pmemobj_create(POOL_FILE, POBJ_LAYOUT_NAME(layout),
		PMEMOBJ_MIN_POOL, S_IRWXU);
	pmemobj_close(pool);
}

static void
obj_cleanup(void)
{
	unlink(POOL_FILE);
}*/

/* size of the pmemblk pool */
// #define	BLK_POOL_SIZE ((off_t)(1 << 25))

/* size of each element in the pmem pool */
// #define	BLK_ELEMENT_SIZE 1024

/*static void
blk_prepare(void)
{
	unlink(POOL_FILE);
	unlink(POOL_FILE_BACKUP);
	PMEMblkpool *pbp = pmemblk_create(POOL_FILE, BLK_ELEMENT_SIZE,
		BLK_POOL_SIZE, 0666);

	if (pbp == NULL)
		pbp = pmemblk_open(POOL_FILE, BLK_ELEMENT_SIZE);

	pmemblk_close(pbp);
}

static void
blk_cleanup(void)
{
	unlink(POOL_FILE);
	unlink(POOL_FILE_BACKUP);
}*/

static void
example(struct pmempool_check_args *args)
{

	PMEMpoolcheck *ppc = pmempool_check_init(args);

	struct pmempool_check_status *status = NULL;
	while ((status = pmempool_check(ppc)) != NULL) {
		switch (status->type) {
		case PMEMPOOL_CHECK_MSG_TYPE_ERROR:
			fprintf(stderr, "%s\n", status->str.msg);
			break;
		case PMEMPOOL_CHECK_MSG_TYPE_INFO:
			printf("%s\n", status->str.msg);
			break;
		case PMEMPOOL_CHECK_MSG_TYPE_QUESTION:
			printf("%s\n", status->str.msg);

			status->str.answer = "yes";
			break;
		default:
			pmempool_check_end(ppc, status);
			exit(EXIT_FAILURE);
		}
	}

	const char *status2str[] = {
		[PMEMPOOL_CHECK_RESULT_CONSISTENT]	= "consistent",
		[PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT] = "not consistent",
		[PMEMPOOL_CHECK_RESULT_REPAIRED]	= "repaired",
		[PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR]	= "cannot repair",
		[PMEMPOOL_CHECK_RESULT_ERROR]		= "fatal",
	};

	enum pmempool_check_result ret = pmempool_check_end(ppc, status);
	printf("status = %s\n", status2str[ret]);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_basic_integration");

	struct pmempool_check_args args = {
		.path		= POOL_FILE,
		.pool_type	= PMEMPOOL_POOL_TYPE_BLK,
		.repair		= true,
		.backup		= false,
		.dry_run	= false,
		.always_yes	= false,
		.flags		= PMEMPOOL_CHECK_FORMAT_STR
				/* | PMEMPOOL_CHECK_FORMAT_DATA */,
		.backup_path	= POOL_FILE_BACKUP
	};
	//blk_prepare();
	example(&args);
	//blk_cleanup();

	args.pool_type = PMEMPOOL_POOL_TYPE_OBJ;
	//obj_prepare();
	example(&args);
	//obj_cleanup();

	DONE(NULL);
}
