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
 * stl.cpp -- example usage of stl c++ bindings
 */
#include <libpmemobj.hpp>

#define	LAYOUT_NAME "stl"

using namespace pmem;
using namespace std;

class A {
public:
	virtual void func() = 0;
};

class B : public A {
public:
	B() {};
	B(int value) : my_value(value) {};

	void func() {
		cout << "class B: " << my_value << endl;
	}

	p<int> my_value;
};

class C : public A {
public:
	C() {};
	C(int value) : my_value(value) {};

	void func() {
		cout << "class C: " << my_value << endl;
	}

	p<int> my_value;
};

class my_root
{
public:
	vector<
		persistent_ptr<A>,
		pmem_allocator_basic<persistent_ptr<A>>
	> pvector;

	p<int> counter;
};

int
main(int argc, char *argv[])
{
	PMEM_REGISTER_TYPE(B);
	PMEM_REGISTER_TYPE(C);

	pool<my_root> pop;
	if (pop.exists(argv[1], LAYOUT_NAME))
		pop.open(argv[1], LAYOUT_NAME);
	else
		pop.create(argv[1], LAYOUT_NAME);

	persistent_ptr<my_root> r = pop.get_root();

	try {
		pop.exec_tx([&] () {
			r->pvector.push_back(make_persistent<B>(r->counter++));
			/* transaction_abort_current(-1); */
			r->pvector.push_back(make_persistent<C>(r->counter++));
		});
	} catch (transaction_error err) {
		cout << err.what() << endl;
	}

	for (auto ptr : r->pvector) {
		ptr->func();
		cout << typeid(*ptr).name() << endl;
	}

	pop.close();
}
