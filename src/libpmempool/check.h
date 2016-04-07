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
 * check.h -- internal definitions for logic performing check
 */

/* queue of check statuses */
struct check_status {
	TAILQ_ENTRY(check_status) next;
	struct pmempool_check_status status;
};

TAILQ_HEAD(check_status_head, check_status);

#define	CHECK_STEPS_COMPLETE	UINT32_MAX

/* container for storing instep location */
#define	CHECK_INSTEP_LOCATION_NUM	2

struct check_instep_location {
	uint64_t instep_location[CHECK_INSTEP_LOCATION_NUM];
};

/* check control context */
struct check_data {
	uint32_t step;
	struct check_instep_location instep_location;

	struct check_status *error;
	struct check_status_head infos;
	struct check_status_head questions;
	struct check_status_head answers;

	struct check_status *check_status_cache;
};

int check_start(PMEMpoolcheck *ppc);
struct check_status *check_step(PMEMpoolcheck *ppc);
void check_stop(PMEMpoolcheck *ppc);

struct check_status *
check_status_create(PMEMpoolcheck *ppc, enum pmempool_check_msg_type type,
	uint32_t question, const char *fmt, ...);
void check_status_push(PMEMpoolcheck *ppc, struct check_status *);
bool check_has_answer(struct check_data *data);
struct check_status *check_pop_answer(struct check_data *data);
struct check_status *check_questions_sequence_validate(PMEMpoolcheck *ppc);
void check_status_release(struct check_status *);

/* create error status */
#define	CHECK_STATUS_ERR(ppc, ...)\
	check_status_create(ppc, PMEMPOOL_CHECK_MSG_TYPE_ERROR, 0, __VA_ARGS__)

/* create question status */
#define	CHECK_STATUS_ASK(ppc, question, ...)\
	check_status_create(ppc, PMEMPOOL_CHECK_MSG_TYPE_QUESTION, question,\
		__VA_ARGS__)

int check_memory(const uint8_t *buff, size_t len, uint8_t val);

const char *check_get_time_str(time_t time);
