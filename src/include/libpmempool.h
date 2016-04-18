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
 * libpmempool.h -- definitions of libpmempool entry points
 *
 * See libpmempool(3) for details.
 */

#ifndef	LIBPMEMPOOL_H
#define	LIBPMEMPOOL_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>

/*
 * pmempool_pool_type -- pool types
 */
enum pmempool_pool_type {
	PMEMPOOL_POOL_TYPE_LOG,
	PMEMPOOL_POOL_TYPE_BLK,
	PMEMPOOL_POOL_TYPE_OBJ,
	PMEMPOOL_POOL_TYPE_BTT_DEV
};

#define	PMEMPOOL_CHECK_FORMAT_STR	(1 << 0)

struct pmempool_check_args {
	const char *path;
	enum pmempool_pool_type pool_type;
	bool repair;
	bool dry_run;
	bool aggresive;
	bool always_yes;
	uint32_t flags;
	const char *backup_path;
};

enum pmempool_check_msg_type {
	PMEMPOOL_CHECK_MSG_TYPE_INFO,
	PMEMPOOL_CHECK_MSG_TYPE_ERROR,
	PMEMPOOL_CHECK_MSG_TYPE_QUESTION,
};

enum pmempool_check_answer {
	PMEMPOOL_CHECK_ANSWER_EMPTY,
	PMEMPOOL_CHECK_ANSWER_YES,
	PMEMPOOL_CHECK_ANSWER_NO,
	PMEMPOOL_CHECK_ANSWER_DEFAULT,
};

enum pmempool_check_result {
	PMEMPOOL_CHECK_RESULT_CONSISTENT,
	PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT,
	PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS,
	PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS,
	PMEMPOOL_CHECK_RESULT_REPAIRED,
	PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR,
	PMEMPOOL_CHECK_RESULT_ERROR,
	PMEMPOOL_CHECK_RESULT_INTERNAL_ERROR
};

struct pmempool_check_status {
	enum pmempool_check_msg_type type;
	uint32_t question;
	enum pmempool_check_answer answer;
	struct {
		const char *msg;
		const char *answer;
	} str;
	struct {
		/*
		 * We would like to implement structure based
		 * communication with user. But this may be implemented
		 * in the future;
		 */
		uint32_t placeholder;
	} data;
};

typedef struct pmempool_check PMEMpoolcheck;

PMEMpoolcheck *pmempool_check_init(struct pmempool_check_args *args);

struct pmempool_check_status *pmempool_check(PMEMpoolcheck *ppc);

enum pmempool_check_result pmempool_check_end(PMEMpoolcheck *ppc,
	struct pmempool_check_status *stat);

/*
 * PMEMPOOL_MAJOR_VERSION and PMEMPOOL_MINOR_VERSION provide the current version
 * of the libpmempool API as provided by this header file.  Applications can
 * verify that the version available at run-time is compatible with the version
 * used at compile-time by passing these defines to pmempool_check_version().
 */
#define	PMEMPOOL_MAJOR_VERSION 0
#define	PMEMPOOL_MINOR_VERSION 1
const char *pmempool_check_version(
		unsigned major_required,
		unsigned minor_required);

const char *pmempool_errormsg(void);

#ifdef __cplusplus
}
#endif
#endif	/* libpmempool.h */
