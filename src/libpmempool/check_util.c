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
 * check_util.c -- check utility functions
 */

#include <stdio.h>
#include <stdint.h>

#include "out.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"

#define	CHECK_END UINT32_MAX

/* separate info part of message from question part of message */
#define	MSG_SEPARATOR	'|'

/* error part of message must have '.' at the end */
#define	MSG_PLACE_OF_SEPARATION	'.'
#define	MAX_MSG_STR_SIZE 8192

#define	CHECK_ANSWER_YES	"yes"
#define	CHECK_ANSWER_NO		"no"

#define	STR_MAX 256
#define	TIME_STR_FMT "%a %b %d %Y %H:%M:%S"

#define	UUID_STR_MAX 37

/* queue of check statuses */
struct check_status {
	TAILQ_ENTRY(check_status) next;
	struct pmempool_check_status status;
	char *msg;
};

TAILQ_HEAD(check_status_head, check_status);

/* check control context */
struct check_data {
	uint32_t step;
	struct check_instep location;

	struct check_status *error;
	struct check_status_head infos;
	struct check_status_head questions;
	struct check_status_head answers;

	struct check_status *check_status_cache;
};

/*
 * check_data_alloc --  allocate and initialize check_data structure
 */
struct check_data *
check_data_alloc()
{
	struct check_data *data = (struct check_data *)malloc(sizeof(*data));
	if (data == NULL) {
		ERR("!malloc");
		return NULL;
	}

	data->check_status_cache = NULL;
	data->error = NULL;
	data->step = 0;

	TAILQ_INIT(&data->infos);
	TAILQ_INIT(&data->questions);
	TAILQ_INIT(&data->answers);

	return data;
}

/*
 * check_data_free -- clean and deallocate check_data
 */
void
check_data_free(struct check_data *data)
{
	if (data->error != NULL) {
		free(data->error);
		data->error = NULL;
	}

	if (data->check_status_cache != NULL) {
		free(data->check_status_cache);
		data->check_status_cache = NULL;
	}

	while (!TAILQ_EMPTY(&data->infos)) {
		struct check_status *statp = TAILQ_FIRST(&data->infos);
		TAILQ_REMOVE(&data->infos, statp, next);
		free(statp);
	}

	while (!TAILQ_EMPTY(&data->questions)) {
		struct check_status *statp = TAILQ_FIRST(&data->questions);
		TAILQ_REMOVE(&data->questions, statp, next);
		free(statp);
	}

	while (!TAILQ_EMPTY(&data->answers)) {
		struct check_status *statp = TAILQ_FIRST(&data->answers);
		TAILQ_REMOVE(&data->answers, statp, next);
		free(statp);
	}

	free(data);
}

/*
 * check_step_get - return current check step number
 */
uint32_t
check_step_get(struct check_data *data)
{
	return data->step;
}

/*
 * check_step_inc -- move to next step number
 */
void
check_step_inc(struct check_data *data)
{
	++data->step;
	memset(&data->location, 0,
		sizeof(struct check_instep));
}

/*
 * check_step_location -- return pointer to structure describing step status
 */
struct check_instep *
check_step_location(struct check_data *data)
{
	return &data->location;
}

/*
 * check_end -- mark check as ended
 */
inline void
check_end(struct check_data *data)
{
	data->step = CHECK_END;
}

/*
 * check_ended -- return if check has ended
 */
inline int
check_ended(struct check_data *data)
{
	return data->step == CHECK_END;
}

/*
 * status_alloc -- (internal) allocate and initialize check_status
 */
static inline struct check_status *
status_alloc()
{
	struct check_status *status = malloc(sizeof(*status));
	if (!status)
		FATAL("!malloc");
	status->msg = malloc(sizeof(char) * MAX_MSG_STR_SIZE);
	if (!status->msg) {
		free(status);
		FATAL("!malloc");
	}
	status->status.str.msg = status->msg;
	status->status.answer = PMEMPOOL_CHECK_ANSWER_EMPTY;
	status->status.question = CHECK_INVALID_QUESTION;
	return status;
}

/*
 * status_release -- (internal) release check_status
 */
static void
status_release(struct check_status *status)
{
	free(status->msg);
	free(status);
}

/*
 * status_msg_trim -- (internal) try to separate info part of the message.
 *	If message is in form of "info.|question" it modifies it as follows
 *	"info\0|question"
 */
static inline int
status_msg_trim(const char *msg)
{
	char *sep = strchr(msg, MSG_SEPARATOR);
	if (sep) {
		ASSERTne(sep, msg);
		--sep;
		ASSERTeq(*sep, MSG_PLACE_OF_SEPARATION);
		*sep = '\0';
		return 0;
	}
	return 1;
}

/*
 * status_msg_prepare -- (internal) if message is in form "info.|question" it
 *	will replace MSG_SEPARATOR '|' with space to get "info. question"
 */
static inline int
status_msg_prepare(const char *msg)
{
	char *sep = strchr(msg, MSG_SEPARATOR);
	if (sep) {
		*sep = ' ';
		return 0;
	}
	return 1;
}

/*
 * check_status_create -- create single status, push it to proper queue
 *	MSG_SEPARATOR character in fmt is treated as message separator. If
 *	creating question but check arguments do not allow to make any changes
 *	(asking any question is pointless) it takes part of message before
 *	MSG_SEPARATOR character and use it to create error message. Character
 *	just before separator must be a MSG_PLACE_OF_SEPARATION character.
 *	Return non 0 value if error status would be created.
 */
int
check_status_create(PMEMpoolcheck *ppc, enum pmempool_check_msg_type type,
	uint32_t question, const char *fmt, ...)
{
	if (!ppc->args.verbose && type == PMEMPOOL_CHECK_MSG_TYPE_INFO)
		return 0;

	struct check_status *st = status_alloc();
	struct check_status *info = NULL;

	if (ppc->args.flags & PMEMPOOL_CHECK_FORMAT_STR) {
		va_list ap;
		va_start(ap, fmt);
		int p = vsnprintf(st->msg, MAX_MSG_STR_SIZE, fmt, ap);
		va_end(ap);

		/* append possible strerror at the end of the message */
		if (type != PMEMPOOL_CHECK_MSG_TYPE_QUESTION && errno &&
				p > 0) {
			snprintf(st->msg + p, MAX_MSG_STR_SIZE - (size_t)p,
				": %s", strerror(errno));
		}

		st->status.type = type;
	}

reprocess:
	switch (st->status.type) {
	case PMEMPOOL_CHECK_MSG_TYPE_ERROR:
		ASSERTeq(ppc->data->error, NULL);
		ppc->data->error = st;
		return -1;

	case PMEMPOOL_CHECK_MSG_TYPE_INFO:
		if (ppc->args.verbose)
			TAILQ_INSERT_TAIL(&ppc->data->infos, st, next);
		else
			check_status_release(ppc, st);
		break;
	case PMEMPOOL_CHECK_MSG_TYPE_QUESTION:

		if (!ppc->args.repair) {
			/* check found issue but cannot perform any repairs */
			if (status_msg_trim(st->msg)) {
				ERR("no error message for the user");
				st->msg[0] = '\0';
			}
			st->status.type = PMEMPOOL_CHECK_MSG_TYPE_ERROR;
			goto reprocess;
		} else if (ppc->args.always_yes) {
			if (!status_msg_trim(st->msg)) {
				/*
				 * have to create two check_statuses
				 * one with information for the user and one
				 * with "yes" answer for further processing
				 */
				info = st;
				info->status.type =
					PMEMPOOL_CHECK_MSG_TYPE_INFO;
				st = status_alloc();
			}
			st->status.question = question;
			ppc->result = PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS;
			st->status.answer = PMEMPOOL_CHECK_ANSWER_YES;
			TAILQ_INSERT_TAIL(&ppc->data->answers, st, next);

			if (info) {
				st = info;
				info = NULL;
				goto reprocess;
			}
		} else {
			/* create simple question message */
			status_msg_prepare(st->msg);
			st->status.question = question;
			ppc->result = PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS;
			st->status.answer = PMEMPOOL_CHECK_ANSWER_EMPTY;
			TAILQ_INSERT_TAIL(&ppc->data->questions, st, next);
		}
		break;
	}

	return 0;
}

/*
 * check_status_release -- release single status object
 */
void
check_status_release(PMEMpoolcheck *ppc, struct check_status *status)
{
	if (status->status.type == PMEMPOOL_CHECK_MSG_TYPE_ERROR)
		ppc->data->error = NULL;

	status_release(status);
}

/*
 * pop_status -- (internal) pop single message from check_status queue
 */
static struct check_status *
pop_status(struct check_data *data, struct check_status_head *queue)
{
	if (!TAILQ_EMPTY(queue)) {
		ASSERTeq(data->check_status_cache, NULL);
		data->check_status_cache = TAILQ_FIRST(queue);
		TAILQ_REMOVE(queue, data->check_status_cache, next);
		return data->check_status_cache;
	}

	return NULL;
}

/*
 * check_pop_question -- pop single question from questions queue
 */
struct check_status *
check_pop_question(struct check_data *data)
{
	return pop_status(data, &data->questions);
}

/*
 * check_pop_info -- pop single info from informations queue
 */
struct check_status *
check_pop_info(struct check_data *data)
{
	return pop_status(data, &data->infos);
}

/*
 * check_pop_error -- pop error from state
 */
struct check_status *
check_pop_error(struct check_data *data)
{
	data->check_status_cache = data->error;
	data->error = NULL;
	return data->check_status_cache;
}

/*
 * check_clear_status_cache -- release check_status from cache
 */
void
check_clear_status_cache(struct check_data *data)
{
	if (data->check_status_cache) {
		enum pmempool_check_msg_type type =
			data->check_status_cache->status.type;
		if (type == PMEMPOOL_CHECK_MSG_TYPE_INFO ||
				type == PMEMPOOL_CHECK_MSG_TYPE_ERROR) {
			status_release(data->check_status_cache);
			data->check_status_cache = NULL;
		}
	}
}

/*
 * status_push -- (internal) push single answer to answers queue
 */
static void
status_push(struct check_data *data, struct check_status *st)
{
	ASSERTeq(st->status.type, PMEMPOOL_CHECK_MSG_TYPE_QUESTION);
	TAILQ_INSERT_TAIL(&data->answers, st, next);
}

/*
 * check_push_answer -- process answer and push it to answers queue
 */
int
check_push_answer(PMEMpoolcheck *ppc)
{
	if (ppc->data->check_status_cache == NULL)
		return 0;

	/* check if answer is "yes" or "no" */
	struct pmempool_check_status *status =
		&ppc->data->check_status_cache->status;
	if (status->str.answer != NULL) {
		if (strcmp(status->str.answer, CHECK_ANSWER_YES) == 0) {
			status->answer = PMEMPOOL_CHECK_ANSWER_YES;
		} else if (strcmp(status->str.answer, CHECK_ANSWER_NO) == 0) {
			status->answer = PMEMPOOL_CHECK_ANSWER_NO;
		}
	}

	if (status->answer == PMEMPOOL_CHECK_ANSWER_EMPTY) {
		/* invalid answer provided */
		status_push(ppc->data, ppc->data->check_status_cache);
		ppc->data->check_status_cache = NULL;
		CHECK_INFO(ppc, "Answer must be either %s or %s",
			CHECK_ANSWER_YES, CHECK_ANSWER_NO);
		return 1;
	} else {
		/* push answer */
		TAILQ_INSERT_TAIL(&ppc->data->answers,
			ppc->data->check_status_cache, next);
		ppc->data->check_status_cache = NULL;
	}

	return 0;
}
/*
 * check_has_error - check if error exists
 */
bool
check_has_error(struct check_data *data)
{
	return data->error != NULL;
}

/*
 * check_has_answer - check if any answer exists
 */
bool
check_has_answer(struct check_data *data)
{
	return !TAILQ_EMPTY(&data->answers);
}

/*
 * pop_answer -- (internal) pop single answer from answers queue
 */
static struct check_status *
pop_answer(struct check_data *data)
{
	struct check_status *ret = NULL;
	if (!TAILQ_EMPTY(&data->answers)) {
		ret = TAILQ_FIRST(&data->answers);
		TAILQ_REMOVE(&data->answers, ret, next);
	}
	return ret;
}

/*
 * check_status_get -- extract pmempool_check_status from check_status
 */
struct pmempool_check_status *
check_status_get(struct check_status *status)
{
	return &status->status;
}

/*
 * check_answer_loop -- loop through all available answers and process them
 */
int
check_answer_loop(PMEMpoolcheck *ppc, struct check_instep *loc, void *ctx,
	int (*callback)(PMEMpoolcheck *, struct check_instep *loc,
	uint32_t question, void *ctx))
{
	struct check_status *answer;

	while ((answer = pop_answer(ppc->data)) != NULL) {
		/* if answer is "no" we cannot fix an issue */
		if (answer->status.answer != PMEMPOOL_CHECK_ANSWER_YES) {
			CHECK_ERR(ppc, "");
			goto cannot_repair;
		}

		/* perform fix */
		if (callback(ppc, loc, answer->status.question, ctx))
			goto cannot_repair;
		if (ppc->result == PMEMPOOL_CHECK_RESULT_ERROR)
			goto error;

		/* fix succeeded */
		ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		check_status_release(ppc, answer);
	}

	return 0;

error:
	check_status_release(ppc, answer);
	return -1;

cannot_repair:
	check_status_release(ppc, answer);
	ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
	return -1;
}

/*
 * check_questions_sequence_validate -- check if sequence of questions resulted
 *	in expected result value and returns if there are any questions for the
 *	user
 */
int
check_questions_sequence_validate(PMEMpoolcheck *ppc)
{
	ASSERT(ppc->result == PMEMPOOL_CHECK_RESULT_CONSISTENT ||
		ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS ||
		ppc->result == PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS ||
		ppc->result == PMEMPOOL_CHECK_RESULT_REPAIRED);
	if (ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS) {
		ASSERT(!TAILQ_EMPTY(&ppc->data->questions));
		return 1;
	} else
		return 0;
}

/*
 * check_memory -- check if memory contains single value
 */
int
check_memory(const uint8_t *buff, size_t len, uint8_t val)
{
	size_t i;
	for (i = 0; i < len; i++) {
		if (buff[i] != val)
			return 1;
	}

	return 0;
}

/*
 * check_get_time_str -- returns time in human-readable format
 */
const char *
check_get_time_str(time_t time)
{
	static char str_buff[STR_MAX] = {0, };
	struct tm *tm = localtime(&time);

	if (tm)
		strftime(str_buff, STR_MAX, TIME_STR_FMT, tm);
	else
		snprintf(str_buff, STR_MAX, "unknown");

	return str_buff;
}

/*
 * check_get_uuid_str -- returns uuid in human readable format
 */
const char *
check_get_uuid_str(uuid_t uuid)
{
	static char uuid_str[UUID_STR_MAX] = {0, };

	int ret = util_uuid_to_string(uuid, uuid_str);
	if (ret != 0) {
		ERR("failed to covert uuid to string");
		return NULL;
	}
	return uuid_str;
}

/*
 * pmempool_check_insert_arena -- insert arena to list
 */
void
check_insert_arena(PMEMpoolcheck *ppc, struct arena *arenap)
{
	TAILQ_INSERT_TAIL(&ppc->pool->arenas, arenap, next);
	ppc->pool->narenas++;
}
