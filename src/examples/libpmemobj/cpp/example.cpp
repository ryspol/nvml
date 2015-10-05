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
#include <libpmemobj.hpp>

#define	LAYOUT_NAME "cpp"

using namespace pmem;
using namespace std;

class foo
{
public:
	foo()
	{
	}

	foo(int val) : bar(val)
	{
		std::cout << "constructor called" << std::endl;
	}

	~foo() {
		std::cout << "destructor called" << std::endl;
	}

	int get_bar() const { return bar; }

	void set_bar(int val) { bar = val; }
private:
	p<int> bar;
};

class A
{
public:
	A() {};
	virtual void func() = 0;
	p<int> var4;
	p<int> var2;
	p<int> va3;
};

class D
{
public:
	D() {};
	virtual void func2() = 0;
	p<int> var;
	p<int> var2;
};

class B : public A, public D
{
public:
	B() {};
	B(int a) {};

	void func() {
		printf("class1 B\n");
	}

	void func2()
	{
		printf("class2 B\n");
	}
};

class C : public A, public D
{
public:
	void func()
	{
		printf("class1 C\n");
	}

	void func2()
	{
		printf("class2 C\n");
	}
};

class my_root
{
public:
	p<int> a; /* transparent wrapper for persistent */
	p<int> b;
	persistent_ptr<A> va;
	pmutex lock;
	persistent_ptr<foo> f; /* smart persistent pointer */
};

int
main(int argc, char *argv[])
{
	PMEM_REGISTER_TYPE(B);

	pool<my_root> pop;
	if (pop.exists(argv[1], LAYOUT_NAME))
		pop.open(argv[1], LAYOUT_NAME);
	else
		pop.create(argv[1], LAYOUT_NAME);

	persistent_ptr<my_root> r = pop.get_root();

	{
		pmutex lock;
		plock_guard<pmutex> guard(pop, lock);
		/* exclusive lock */
	}

	{
		pshared_mutex lock;
		{
			pshared_lock<pshared_mutex> guard(pop, lock);
			/* read lock */
		}

		{
			plock_guard<pshared_mutex> guard(pop, lock);
			/* write lock */
		}
	}

	/* scoped transaction */
	try {
		transaction tx(pop, r->lock);
		r->a = 5;
		r->b = 10;
		if (r->f != nullptr)
			delete_persistent(r->f);
	} catch (exception &e) {
		cout << e.what() << endl;
		transaction_abort_current(-1);
	}

	if (r->va != nullptr) {
		r->va->func();
	}

	pshared_mutex lock;

	/* lambda transaction */
	pop.exec_tx(lock, [&] () {
		auto f = make_persistent<foo>(15);
		make_persistent<foo>(1);
		make_persistent<foo>(2);
		r->f = f;
		delete_persistent(r->va);
		r->va = make_persistent<B>();
	});

	try {
		/* aborting lambda transaction */
		pop.exec_tx(lock, [&] () {
			p<int> test = 10;
			int itest = test;
			r->a = itest;
			transaction_abort_current(-1);
			assert(0);
		});
	} catch (transaction_error &e) {
		cout << e.what() << endl;
	}
	assert(r->a == 5);

	/* aborting scoped transaction */
	try {
		transaction tx(pop);
		r->a = 10;
		transaction_abort_current(-1);
		assert(0);
	} catch (transaction_error &e) {
		cout << e.what() << endl;
	}
	assert(r->a == 5);

	for (auto itr = cbegin_obj<foo>(pop); itr != cend_obj<foo>(); itr++) {
		cout << itr->get_bar() << endl;
	}

	for (auto itr = begin_obj<foo>(pop); itr != end_obj<foo>(); itr++) {
		itr->set_bar(10);
	}

	assert(r->a == 5);
	assert(r->b == 10);
	assert(r->f->get_bar() == 10);

	pop.close();
}
