/*
 * Copyright (c) 2014-2015, Intel Corporation
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

 #ifndef	LIBPMEMOBJPP_H
 #define	LIBPMEMOBJPP_H 1

#include <libpmemobj.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <stddef.h>
#include <sys/stat.h>
#include <memory>
#include <assert.h>
#include <algorithm>
#include <exception>

namespace pmem {
	class transaction_error : public std::runtime_error {
	public:
		using std::runtime_error::runtime_error;
	};

	class transaction_alloc_error : public transaction_error {
	public:
		using transaction_error::transaction_error;
	};

	class transaction_scope_error : public std::logic_error {
	public:
		using std::logic_error::logic_error;
	};

	class pool_error : public std::runtime_error {
	public:
		using std::runtime_error::runtime_error;
	};

	class ptr_error : public std::logic_error {
	public:
		using std::logic_error::logic_error;
	};

	class lock_error : public std::logic_error {
	public:
		using std::logic_error::logic_error;
	};

	template<typename T>
	class pool;

	template<typename T>
	class persistent_ptr
	{
		template<typename A>
		friend void delete_persistent(persistent_ptr<A> ptr);

		template<typename A, typename R>
		friend void make_persistent_atomic(pool<R> pop,
			persistent_ptr<A> &ptr);
	public:
		persistent_ptr() : oid(OID_NULL)
		{
		}
		persistent_ptr(PMEMoid _oid) : oid(_oid)
		{
		}

		/* don't allow volatile allocations */
		void *operator new(std::size_t size);
		void operator delete(void *ptr, std::size_t size);

		inline persistent_ptr& operator=(persistent_ptr rhs) {
			std::swap(oid, rhs.oid);
			if (pmemobj_tx_stage() == TX_STAGE_WORK)
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));

			return *this;
		}

		inline T* get() const
		{
			return (T*)pmemobj_direct(oid);
		}
		inline T* operator->()
		{
			return get();
		}
		inline T& operator*()
		{
			return *get();
		}
		inline const T* operator->() const
		{
			return get();
		}
		inline const T& operator*() const
		{
			return *get();
		}
		inline size_t usable_size() const
		{
			return pmemobj_alloc_usable_size(oid);
		}

		typedef T* (persistent_ptr<T>::*unspecified_bool_type)() const;
		operator unspecified_bool_type() const
		{
			return OID_IS_NULL(oid) ? 0 : &persistent_ptr<T>::get;
		}
	private:
		PMEMoid oid;
	};

	class base_pool
	{
		friend class transaction;
		friend class mutex;
		friend class shared_mutex;

		template<typename T>
		friend
		void make_persistent_atomic(base_pool &p,
			persistent_ptr<T> &ptr);
	public:
		void exec_tx(std::function<void ()> tx)
		{
			if (pmemobj_tx_begin(pop, NULL, TX_LOCK_NONE) != 0)
				throw transaction_error(
					"failed to start transaction");
			try {
				tx();
			} catch (...) {
				if (pmemobj_tx_stage() == TX_STAGE_WORK) {
					pmemobj_tx_abort(-1);
				}
				throw;
			}

			if (pmemobj_tx_process() != 0)
				throw transaction_error("failed to process"
					"transaction");

			pmemobj_tx_end();
		}

		/* XXX variadic arguments/variadic template */
		template<typename L>
		void exec_tx(L &l, std::function<void ()> tx)
		{
			if (pmemobj_tx_begin(pop, NULL,
				l.lock_type(), &l.plock, TX_LOCK_NONE) != 0)
				throw transaction_error(
					"failed to start transaction");
			try {
				tx();
			} catch (...) {
				if (pmemobj_tx_stage() == TX_STAGE_WORK) {
					pmemobj_tx_abort(-1);
				}
				throw;
			}

			if (pmemobj_tx_process() != 0)
				throw transaction_error("failed to process"
					"transaction");

			pmemobj_tx_end();
		}
	protected:
		PMEMobjpool *pop;
	};

	template<typename T>
	class pool : public base_pool
	{
	public:
		persistent_ptr<T> get_root()
		{
			persistent_ptr<T> root =  pmemobj_root(pop, sizeof (T));

			return root;
		}

		void open(std::string path, std::string layout)
		{
			pop = pmemobj_open(path.c_str(), layout.c_str());
			if (pop == nullptr)
				throw pool_error("failed to open the pool");
		}

		void create(std::string path, std::string layout,
			std::size_t size = PMEMOBJ_MIN_POOL,
			mode_t mode = S_IWUSR | S_IRUSR)
		{
			pop = pmemobj_create(path.c_str(), layout.c_str(),
				size, mode);
			if (pop == nullptr)
				throw pool_error("failed to create the pool");
		}

		int check(std::string path, std::string layout)
		{
			return pmemobj_check(path.c_str(), layout.c_str());
		}

		int exists(std::string path, std::string layout)
		{
			return access(path.c_str(), F_OK) == 0 &&
				check(path, layout);
		}

		void close()
		{
			if (pop == nullptr)
				throw std::logic_error("pool already closed");
			pmemobj_close(pop);
		}
	};

	class mutex
	{
		friend class base_pool;
		friend class transaction;
	public:
		void lock(base_pool &pop)
		{
			if (pmemobj_mutex_lock(pop.pop, &plock) != 0)
				throw lock_error("failed to lock a mutex");
		}

		bool try_lock(base_pool &pop)
		{
			return pmemobj_mutex_trylock(pop.pop, &plock);
		}

		void unlock(base_pool &pop)
		{
			if (pmemobj_mutex_unlock(pop.pop, &plock) != 0)
				throw lock_error("failed to unlock a mutex");
		}

		enum pobj_tx_lock lock_type()
		{
			return TX_LOCK_MUTEX;
		}
	private:
		PMEMmutex plock;
	};

	class shared_mutex
	{
		friend class base_pool;
		friend class transaction;
	public:
		void lock(base_pool &pop)
		{
			if (pmemobj_rwlock_rdlock(pop.pop, &plock) != 0)
				throw lock_error("failed to read lock a"
						"shared mutex");
		}

		void lock_shared(base_pool &pop)
		{
			if (pmemobj_rwlock_wrlock(pop.pop, &plock) != 0)
				throw lock_error("failed to write lock a"
						"shared mutex");
		}

		int try_lock(base_pool &pop)
		{
			return pmemobj_rwlock_trywrlock(pop.pop, &plock);
		}

		int try_lock_shared(base_pool &pop)
		{
			return pmemobj_rwlock_tryrdlock(pop.pop, &plock);
		}

		void unlock(base_pool &pop)
		{
			if (pmemobj_rwlock_unlock(pop.pop, &plock) != 0)
				throw lock_error("failed to unlock a"
						"shared mutex");
		}

		enum pobj_tx_lock lock_type()
		{
			return TX_LOCK_RWLOCK;
		}
	private:
		PMEMrwlock plock;
	};

	class transaction
	{
	public:
		transaction(base_pool &p)
		{
			if (pmemobj_tx_begin(p.pop, NULL, TX_LOCK_NONE) != 0)
				throw transaction_error(
					"failed to start transaction");
		}

		/* XXX variadic arguments/variadic template */
		template<typename L>
		transaction(base_pool &p, L &l)
		{
			if (pmemobj_tx_begin(p.pop, NULL,
				l.lock_type(), &l.plock, TX_LOCK_NONE) != 0)
				throw transaction_error(
					"failed to start transaction");
		}

		~transaction()
		{
			/* can't throw from destructor */
			assert(pmemobj_tx_process() == 0);
			pmemobj_tx_end();
		}

		void abort(int err)
		{
			pmemobj_tx_abort(err);
			throw transaction_error("explicit abort " +
						std::to_string(err));
		}
	};

	void transaction_abort_current(int err)
	{
		pmemobj_tx_abort(err);
	}

	template<typename T>
	class p
	{
		/* don't allow volatile allocations */
		void *operator new(std::size_t size);
		void operator delete(void *ptr, std::size_t size);
	public:
		p(T _val) : val(_val)
		{
		}
		inline p& operator=(p rhs) {
			std::swap(val, rhs.val);
			if (pmemobj_tx_stage() == TX_STAGE_WORK)
				pmemobj_tx_add_range_direct(this, sizeof(T));

			return *this;
		}
		inline operator T() const
		{
			return val;
		}
	private:
		T val;
	};

	template<typename T>
	persistent_ptr<T> make_persistent()
	{
		persistent_ptr<T> ptr = pmemobj_tx_alloc(sizeof (T), 0);

		new (ptr.get()) T();

		return ptr;
	}

	template<typename T, typename... Args>
	persistent_ptr<T> make_persistent(Args && ... args)
	{
		if (pmemobj_tx_stage() != TX_STAGE_WORK)
			throw transaction_scope_error("refusing to allocate"
				"memory outside of transaction scope");

		persistent_ptr<T> ptr = pmemobj_tx_alloc(sizeof (T), 0);
		if (ptr == nullptr)
			throw transaction_alloc_error("failed to allocate"
				"persistent memory object");

		new (ptr.get()) T(args...);

		return ptr;
	}

	template<typename T>
	void delete_persistent(persistent_ptr<T> ptr)
	{
		if (ptr == nullptr)
			return;

		if (pmemobj_tx_free(ptr.oid) != 0)
			throw transaction_alloc_error("failed to delete"
				"persistent memory object");

		ptr->T::~T();

		ptr.oid = OID_NULL;
	}

	template<typename T>
	void obj_constructor(void *ptr, void *arg)
	{
		/* XXX: need to pack parameters for constructor in args */
		new (ptr) T();
	}

	template<typename T>
	void make_persistent_atomic(base_pool &p, persistent_ptr<T> &ptr)
	{
		pmemobj_alloc(p.pop, &ptr->oid, sizeof (T), 0,
			&obj_constructor<T>, NULL);
	}

	template<typename T>
	void delete_persistent_atomic(persistent_ptr<T> &ptr)
	{
		/* we CAN'T call destructor */
		pmemobj_free(&ptr->oid);
	}

	template<typename T>
	class lock_guard
	{
	public:
		lock_guard(base_pool &_pop, T &_l) : lockable(_l), pop(_pop)
		{
			lockable.lock(pop);
		};

		~lock_guard()
		{
			lockable.unlock(pop);
		};
	private:
		T &lockable;
		base_pool &pop;
	};

	template<typename T>
	class shared_lock
	{
	public:
		shared_lock(base_pool &_pop, T &_l) : lockable(_l), pop(_pop)
		{
			lockable.lock_shared(pop);
		};

		~shared_lock()
		{
			lockable.unlock(pop);
		};
	private:
		T &lockable;
		base_pool &pop;
	};
}

#endif	/* LIBPMEMOBJPP_H */
