/*
 * Copyright (c) 2015, Intel Corporation
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
 * cpp.c -- example usage of cpp allocations
 */
#include <libpmemobjpp.h>

#define	LAYOUT_NAME "cpp"

using namespace pmem;
using namespace std;

class foo {
public:
	foo(int val) : bar(val)
	{
		std::cout << "constructor called" << std::endl;
	}
	~foo() {
		std::cout << "destructor called" << std::endl;
	}
	int get_bar() { return bar; }
	void set_bar(int val) { bar = val; }
private:
	p<int> bar;
};

class my_root {
public:
	p<int> a; /* transparent wrapper for persistent */
	p<int> b;
	mutex lock;
	persistent_ptr<foo> f; /* smart persistent pointer */
};

int
main(int argc, char *argv[])
{
	pool<my_root> pop;
	if (pop.exists(argv[1], LAYOUT_NAME))
		pop.open(argv[1], LAYOUT_NAME);
	else
		pop.create(argv[1], LAYOUT_NAME);

	persistent_ptr<my_root> r = pop.get_root();

	{
		mutex lock;
		lock_guard<mutex> guard(pop, lock);
		/* exclusive lock */
	}

	{
		shared_mutex lock;
		{
			shared_lock<shared_mutex> guard(pop, lock);
			/* read lock */
		}

		{
			lock_guard<shared_mutex> guard(pop, lock);
			/* write lock */
		}
	}

	/* scoped transaction */
	try {
		transaction tx(pop, r->lock);
		r->a = 5;
		r->b = 10;
		if (r->f != nullptr) {
			delete_persistent(r->f);
		}
	} catch (...) {
		transaction_abort_current(-1);
	}

	shared_mutex lock;
	/* lambda transaction */
	pop.exec_tx(lock, [&] () {
		auto f = make_persistent<foo>(15);
		r->f = f;
	});

	assert(r->a == 5);
	assert(r->b == 10);
	assert(r->f->get_bar() == 15);

	pop.close();
}
