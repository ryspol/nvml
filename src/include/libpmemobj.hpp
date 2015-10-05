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
#include <unordered_map>
#include <thread>
#include <mutex>
#include <map>
#include <list>
#include <type_traits>
#include <vector>
#include <deque>
#include <queue>
#include <set>

static uint64_t pool_uuid;
static uint64_t pool_base;

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

	class type_error : public std::logic_error {
	public:
		using std::logic_error::logic_error;
	};

	class ptr_error : public std::logic_error {
	public:
		using std::logic_error::logic_error;
	};

	class lock_error : public std::logic_error {
	public:
		using std::logic_error::logic_error;
	};

	typedef std::pair<size_t, uintptr_t> vtable_entry;
	typedef std::list<vtable_entry> type_vptrs;
	static std::map<uint64_t, type_vptrs> pmem_types;

	template<typename T>
	static uint64_t type_num()
	{
		return typeid(T).hash_code() % 1024;
	}

	template<typename T>
	static void register_type(T *ptr)
	{
		type_vptrs ptrs;

		if (pmem_types.find(type_num<T>()) != pmem_types.end())
			throw type_error("type already registered");

		uintptr_t *p = (uintptr_t *)(ptr);
		for (size_t i = 0; i < sizeof (T) / sizeof (uintptr_t); ++i)
			if (p[i] != 0)
				ptrs.push_back({i, p[i]});

		pmem_types[type_num<T>()] = ptrs;
	}

	class base_pool;

	template<typename T>
	class pool;

	template< class T > struct sp_element
	{
		typedef T type;
	};

	template< class T > struct sp_element< T[] >
	{
		typedef T type;
	};

	template< class T, std::size_t N > struct sp_element< T[N] >
	{
		typedef T type;
	};

	template< class T > struct sp_dereference
	{
		typedef T & type;
	};

	template<> struct sp_dereference< void >
	{
		typedef void type;
	};

	template<> struct sp_dereference< void const >
	{
		typedef void type;
	};

	template<> struct sp_dereference< void volatile >
	{
		typedef void type;
	};

	template<> struct sp_dereference< void const volatile >
	{
		typedef void type;
	};

	template< class T > struct sp_dereference< T[] >
	{
		typedef T type;
	};

	template< class T, std::size_t N > struct sp_dereference< T[N] >
	{
		typedef T type;
	};

	template< class T > struct sp_array_access
	{
		typedef T & type;
	};

	template< class T > struct sp_member_access
	{
		typedef T * type;
	};

	template< class T > struct sp_member_access< T[] >
	{
		typedef T type;
	};

	template< class T, std::size_t N > struct sp_member_access< T[N] >
	{
		typedef T type;
	};

	template< class T > struct sp_array_access< T[] >
	{
		typedef T & type;
	};

	template<> struct sp_array_access< void >
	{
		typedef void type;
	};

	template< class T, std::size_t N > struct sp_array_access< T[N] >
	{
		typedef T & type;
	};

	template< class T > struct sp_extent
	{
		enum _vt { value = 0 };
	};

	template< class T, std::size_t N > struct sp_extent< T[N] >
	{
		enum _vt { value = N };
	};

	template<typename T>
	class persistent_ptr
	{
		typedef typename sp_element< T >::type element_type;

		template<typename A>
		friend void delete_persistent(persistent_ptr<A> ptr);

		template<typename A>
		friend void make_persistent_atomic(base_pool &pop,
			persistent_ptr<A> &ptr);

		template<typename A>
		friend void delete_persistent_atomic(persistent_ptr<A> &ptr);

		template<typename A>
		friend void delete_persistent_array(persistent_ptr<A[]> ptr, std::size_t N);

		template<typename V>
		friend class persistent_ptr;

		template<typename I, typename UI>
		friend class piterator;
	public:
		persistent_ptr() = default;

		persistent_ptr(PMEMoid _oid) : oid(_oid)
		{
		}

		persistent_ptr(std::nullptr_t null) : oid(OID_NULL)
		{
		}

		template <typename V>
		persistent_ptr(persistent_ptr<V> const &rhs) noexcept
		{
			//static_assert(std::is_assignable<element_type, typename sp_element< V >::type>::value,
			//	"assigning incompatible persistent types");

			oid = rhs.oid;
		}

		/*~persistent_ptr() noexcept {
		}*/

		/* don't allow volatile allocations */
		void *operator new(std::size_t size);
		void operator delete(void *ptr, std::size_t size);

		inline persistent_ptr& operator=(persistent_ptr const &rhs) noexcept
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}

			oid = rhs.oid;

			return *this;
		}

		template<typename V>
		inline persistent_ptr& operator=(persistent_ptr<V> const &rhs) noexcept
		{
			//static_assert(std::is_assignable<element_type, typename sp_element< V >::type>::value,
			//	"assigning incompatible persistent types");

			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}

			oid = rhs.oid;

			return *this;
		}

		inline persistent_ptr& operator=(std::nullptr_t) noexcept
		{
			oid = OID_NULL;

			return *this;
		}

		inline element_type* get() const
		{
			pool_uuid = oid.pool_uuid_lo;
			uintptr_t *d = (uintptr_t *)pmemobj_direct(oid);
			/* do this once per run (add runid) */
			if (d && std::is_polymorphic<T>::value) {
				fix_vtables();
			}

			return (element_type*)d;
		}

		inline typename sp_member_access<T>::type operator->() const
		{
			return get();
		}

		inline typename sp_dereference<T>::type operator*() const
		{
			return *get();
		}

		inline typename sp_array_access<T>::type operator[] (std::ptrdiff_t i) const
		{
			return get()[i];
		}

		inline size_t usable_size() const
		{
			return pmemobj_alloc_usable_size(oid);
		}

		typedef element_type* (persistent_ptr<T>::*unspecified_bool_type)() const;
		operator unspecified_bool_type() const
		{
			return OID_IS_NULL(oid) ? 0 : &persistent_ptr<T>::get;
		}

		explicit operator bool() const
		{
			return get() != nullptr;
		}

		template<typename U>
		inline bool operator==(persistent_ptr<U> const &b) const
		{
			return oid.off == b.oid.off;
		}

		template<typename U>
		inline bool operator!=(persistent_ptr<U> const &b) const
		{
			return oid.off != b.oid.off;
		}

		template<typename U>
		inline bool operator==(persistent_ptr<U> &b)
		{
			return oid.off == b.oid.off;
		}

		template<typename U>
		inline bool operator!=(persistent_ptr<U> &b)
		{
			return oid.off != b.oid.off;
		}

		template<typename U>
		inline bool operator>(persistent_ptr<U> &b)
		{
			return oid.off > b.oid.off;
		}

		template<typename U>
		inline bool operator<(persistent_ptr<U> &b)
		{
			return oid.off < b.oid.off;
		}

		inline persistent_ptr<T>& operator++()
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}
			oid.off += sizeof(T);
			return *this;
		}

		inline persistent_ptr<T> operator++(int)
		{
			PMEMoid noid = oid;
			oid.off += sizeof(T);

			return noid;
		}

		inline persistent_ptr<T>& operator+=(const persistent_ptr &rhs)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}
			oid.off += rhs.off;
			return *this;
		}

		inline persistent_ptr<T>& operator+=(std::ptrdiff_t s)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}
			oid.off += (s * sizeof(T));
			return *this;
		}

		inline persistent_ptr<T>& operator-=(std::ptrdiff_t s)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}
			oid.off -= (s * sizeof(T));
			return *this;
		}

		inline persistent_ptr<T> operator+(std::size_t s)
		{
			PMEMoid noid;
			noid.pool_uuid_lo = oid.pool_uuid_lo;
			noid.off = oid.off + (s * sizeof(T));
			return persistent_ptr<T>(noid);
		}

		inline persistent_ptr<T> operator-(std::size_t s) const
		{
			PMEMoid noid;
			noid.pool_uuid_lo = oid.pool_uuid_lo;
			noid.off = oid.off - (s * sizeof(T));
			return persistent_ptr<T>(noid);
		}

		inline ptrdiff_t operator-(persistent_ptr const &rhs) const
		{
			ptrdiff_t d = oid.off - rhs.oid.off;

			return d / sizeof (T);
		}

		inline persistent_ptr<T>& operator--()
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}
			oid.off -= sizeof(T);
			return *this;
		}

	private:
		void fix_vtables() const
		{
			uintptr_t *d = (uintptr_t *)pmemobj_direct(oid);
			type_vptrs vptrs = pmem_types[pmemobj_type_num(oid)];
			for (vtable_entry e : vptrs) {
				d[e.first] = e.second;
			}
		}
		PMEMoid oid;
	};

	template<typename T, typename UT = typename std::remove_const<T>::type>
	class piterator : public std::iterator<std::forward_iterator_tag,
				UT, std::ptrdiff_t, T*, T&>
	{
		template<typename I>
		friend piterator<I> begin_obj(base_pool &pop);
		template<typename I>
		friend piterator<const I> cbegin_obj(base_pool &pop);

		template<typename I>
		friend piterator<I> end_obj();
		template<typename I>
		friend piterator<const I> cend_obj();

		template<typename I, typename UI>
		friend class piterator;
	public:
		piterator() : itr(nullptr)
		{
		}

		void swap(piterator& other) noexcept
		{
			std::swap(itr, other.itr);
		}

		piterator& operator++()
		{
			if (itr == nullptr)
				throw std::out_of_range(
					"iterator out of bounds");

			itr = pmemobj_next(itr.oid);
			return *this;
		}

		piterator operator++(int)
		{
			if (itr == nullptr)
				throw std::out_of_range(
					"iterator out of bounds");

			piterator tmp(*this);
			itr = pmemobj_next(itr.oid);
			return tmp;
		}

		template<typename I>
		bool operator==(const piterator<I> &rhs) const
		{
			return itr == rhs.itr;
		}

		template<typename I>
		bool operator!=(const piterator<I> &rhs) const
		{
			return itr != rhs.itr;
		}

		inline T& operator*() const
		{
			if (itr == nullptr)
				throw ptr_error("dereferencing null iterator");

			return *itr.get();
		}

		inline T* operator->() const
		{
			return &(operator*());
		}

		operator piterator<const T>() const
		{
			return piterator<const T>(itr);
		}
	private:
		persistent_ptr<UT> itr;

		piterator(persistent_ptr<UT> p) : itr(p)
		{
		}
	};

	class base_pool
	{
		friend class transaction;
		friend class pmutex;
		friend class pshared_mutex;
		friend class pconditional_variable;

		template<typename T>
		friend void make_persistent_atomic(base_pool &p,
			persistent_ptr<T> &ptr);

		template<typename T>
		friend piterator<T> begin_obj(base_pool &pop);

		template<typename T>
		friend piterator<const T> cbegin_obj(base_pool &pop);
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
				throw transaction_error("failed to process "
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
				pmemobj_tx_end();
				throw;
			}

			if (pmemobj_tx_process() != 0) {
				pmemobj_tx_end();
				throw transaction_error("failed to process "
					"transaction");
			}

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
			pool_base = (uint64_t)pop;
		}

		void create(std::string path, std::string layout,
			std::size_t size = PMEMOBJ_MIN_POOL,
			mode_t mode = S_IWUSR | S_IRUSR)
		{
			pop = pmemobj_create(path.c_str(), layout.c_str(),
				size, mode);
			if (pop == nullptr)
				throw pool_error("failed to create the pool");
			pool_base = (uint64_t)pop;
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

	class pmutex
	{
		friend class base_pool;
		friend class transaction;
		friend class pconditional_variable;
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

	class pshared_mutex
	{
		friend class base_pool;
		friend class transaction;
	public:
		void lock(base_pool &pop)
		{
			if (pmemobj_rwlock_rdlock(pop.pop, &plock) != 0)
				throw lock_error("failed to read lock a "
						"shared mutex");
		}

		void lock_shared(base_pool &pop)
		{
			if (pmemobj_rwlock_wrlock(pop.pop, &plock) != 0)
				throw lock_error("failed to write lock a "
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
				throw lock_error("failed to unlock a "
						"shared mutex");
		}

		enum pobj_tx_lock lock_type()
		{
			return TX_LOCK_RWLOCK;
		}
	private:
		PMEMrwlock plock;
	};

	class pconditional_variable
	{
	public:
		void notify_one(base_pool &pop)
		{
			pmemobj_cond_signal(pop.pop, &pcond);
		}

		void notify_all(base_pool &pop)
		{
			pmemobj_cond_broadcast(pop.pop, &pcond);
		}

		void wait(base_pool &pop, pmutex &lock)
		{
			pmemobj_cond_wait(pop.pop, &pcond, &lock.plock);
		}
	private:
		PMEMcond pcond;
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
		throw transaction_error("explicit abort " +
					std::to_string(err));
	}

	template<typename T>
	piterator<T> begin_obj(base_pool &pop)
	{
		persistent_ptr<T> p = pmemobj_first(pop.pop, type_num<T>());
		return p;
	}

	template<typename T>
	piterator<T> end_obj()
	{
		persistent_ptr<T> p;
		return p;
	}

	template<typename T>
	piterator<const T> cbegin_obj(base_pool &pop)
	{
		persistent_ptr<T> p = pmemobj_first(pop.pop, type_num<T>());
		return p;
	}

	template<typename T>
	piterator<const T> cend_obj()
	{
		persistent_ptr<T> p;
		return p;
	}

	template<typename T>
	class p
	{
		/* don't allow volatile allocations */
//		void *operator new(std::size_t size);
//		void operator delete(void *ptr, std::size_t size);
	public:
		p(T _val) : val(_val)
		{
		}
		p() = default;
		~p() = default;

		inline p& operator=(p rhs)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK)
				pmemobj_tx_add_range_direct(this, sizeof(T));

			std::swap(val, rhs.val);

			return *this;
		}

		inline operator T() const
		{
			return val;
		}

		p& operator++()
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK)
				pmemobj_tx_add_range_direct(this, sizeof(T));

			val++;
			return *this;
		}

		p operator++(int)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}

			p<T> tmp = *this;
			++*this;

			return tmp;
		}

		inline p<T>& operator-=(T s)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}
			val -= s;
			return *this;
		}
	private:
		T val;
	};

	template<typename T>
	persistent_ptr<T> make_persistent(size_t s = sizeof(T))
	{
		persistent_ptr<T> ptr = pmemobj_tx_alloc(s,
			type_num<T>());

		::new (ptr.get()) T();

		return ptr;
	}

	template<typename T>
	persistent_ptr<T[]> make_persistent_array(std::size_t N)
	{
		persistent_ptr<T[]> ptr = pmemobj_tx_alloc(sizeof (T) * N,
			type_num<T>());

		for (int i = 0; i < N; ++i)
			::new (ptr.get() + i) T();

		return ptr;
	}

	template<typename T>
	void delete_persistent_array(persistent_ptr<T[]> ptr, std::size_t N)
	{
		if (ptr == nullptr)
			return;

		if (pmemobj_tx_free(ptr.oid) != 0)
			throw transaction_alloc_error("failed to delete "
				"persistent memory object");

		for (int i = 0; i < N; ++i)
			ptr[i].T::~T();

		ptr.oid = OID_NULL;
	}

	template<typename T, typename... Args>
	persistent_ptr<T> make_persistent(Args && ... args)
	{
		if (pmemobj_tx_stage() != TX_STAGE_WORK)
			throw transaction_scope_error("refusing to allocate "
				"memory outside of transaction scope");

		persistent_ptr<T> ptr = pmemobj_tx_alloc(sizeof (T),
			type_num<T>());

		if (ptr == nullptr)
			throw transaction_alloc_error("failed to allocate "
				"persistent memory object");

		new (ptr.get()) T(args...);

		return ptr;
	}

	template<typename T>
	void delete_persistent(persistent_ptr<T> ptr)
	{
		if (ptr == nullptr)
			return;

		ptr->T::~T();

		if (pmemobj_tx_free(ptr.oid) != 0)
			throw transaction_alloc_error("failed to delete "
				"persistent memory object");

		ptr.oid = OID_NULL;
	}

	template<typename T>
	void obj_constructor(PMEMobjpool *pop, void *ptr, void *arg)
	{
		/* XXX: need to pack parameters for constructor in args */
		new (ptr) T();
	}

	template<typename T>
	void make_persistent_atomic(base_pool &p, persistent_ptr<T> &ptr)
	{
		pmemobj_alloc(p.pop, &ptr.oid, sizeof (T), type_num<T>(),
			&obj_constructor<T>, NULL);
	}

	template<typename T>
	void delete_persistent_atomic(persistent_ptr<T> &ptr)
	{
		/* we CAN'T call destructor */
		pmemobj_free(&ptr.oid);
	}

	template<typename T>
	class plock_guard
	{
	public:
		plock_guard(base_pool &_pop, T &_l) : lockable(_l), pop(_pop)
		{
			lockable.lock(pop);
		};

		~plock_guard()
		{
			lockable.unlock(pop);
		};
	private:
		T &lockable;
		base_pool &pop;
	};

	template<typename T>
	class pshared_lock
	{
	public:
		pshared_lock(base_pool &_pop, T &_l) : lockable(_l), pop(_pop)
		{
			lockable.lock_shared(pop);
		};

		~pshared_lock()
		{
			lockable.unlock(pop);
		};
	private:
		T &lockable;
		base_pool &pop;
	};

	class pstring {
	public:
		pstring() : arr(OID_NULL) {

		}

		pstring(const char *str) {
			int len = strlen(str) + 1;
			arr = make_persistent_array<char>(len);
			memcpy(arr.get(), str, len);
		}

		inline operator std::string() const {
			return std::string(arr.get());
		}

		persistent_ptr<char[]> arr;
	};
}

namespace std {
	template <typename T> struct iterator_traits<typename pmem::persistent_ptr<T>>
	{
		using value_type = T;
		using pointer = pmem::persistent_ptr<T>;
		using reference = T&;
		using size_type = std::size_t;
		using difference_type = std::ptrdiff_t;
		using iterator_category = std::random_access_iterator_tag;
	};
	template <typename T> struct iterator_traits<typename pmem::persistent_ptr<const T>>
	{
		using value_type = T;
		using pointer = const pmem::persistent_ptr<T>;
		using reference = const T&;
		using size_type = std::size_t;
		using difference_type = std::ptrdiff_t;
		using iterator_category = std::random_access_iterator_tag;
	};

	template <typename T>
	struct pointer_traits<typename pmem::persistent_ptr<T>>
	{
		using pointer = pmem::persistent_ptr<T>;
		using element_type = T;
		using difference_type = std::ptrdiff_t;
		static pointer pointer_to( element_type &r ) {
			PMEMoid oid;
			oid.off = (uint64_t)&r - pool_base;
			oid.pool_uuid_lo = pool_uuid;
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(pmemobj_direct(oid),
					sizeof (T));
			}
			return oid;
		}
		template <class U> using rebind = pmem::persistent_ptr<U>;
		using bool_type = pmem::p<bool>;
	};

	template <typename T>
	struct pointer_traits<typename pmem::persistent_ptr<T> const>
	{
		using pointer = const pmem::persistent_ptr<T>;
		using element_type = const T;
		using difference_type = std::ptrdiff_t;
		static pointer pointer_to( element_type const &r ) {
			PMEMoid oid;
			oid.off = (uint64_t)&r - pool_base;
			oid.pool_uuid_lo = pool_uuid;
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(pmemobj_direct(oid),
					sizeof (T));
			}
			return oid;
		}
		template <class U> using rebind = pmem::persistent_ptr<const U>;
		using bool_type = pmem::p<bool>;
	};

	template <>
	struct pointer_traits<typename pmem::persistent_ptr<void>>
	{
		using pointer = pmem::persistent_ptr<void>;
		using element_type = void;
		using difference_type = pmem::p<std::ptrdiff_t>;
		template <class U> using rebind = pmem::persistent_ptr<U>;
	};
}

namespace pmem {
	template <typename T>
	class pmem_allocator
	{
	public:

		using value_type = T;
		using pointer = persistent_ptr<T>;
		using const_pointer = const persistent_ptr<T>;
		using reference = T&;
		using const_reference = const T&;
		using size_type = p<std::size_t>;
		using difference_type = std::ptrdiff_t;
		using bool_type = p<bool>;

		template <class U>
		struct rebind { using other = pmem_allocator<U>; };

		pmem_allocator() {};
		pmem_allocator(const pmem_allocator&) {};
		template <class U>
		pmem_allocator(const pmem_allocator<U>&) {};
		template <class U>
		pmem_allocator(const pmem_allocator<U>&&) {};


		pmem_allocator& operator=(const pmem_allocator& other) = delete;
		~pmem_allocator() {};

//		pointer address(reference r) const { return &r; };
		const_pointer address(const_reference r) const
		{
			return &r;
		};

		bool operator!=(const pmem_allocator& other)
		{
			return !(*this == other);
		};

		bool operator==(const pmem_allocator&)
		{
			return true;
		 };

		pointer allocate(size_type n,
			std::allocator<void>::const_pointer = 0) const
		{
			return make_persistent<T>(sizeof(T) * n);
		};

		void deallocate(pointer p, size_type n)
		{
			delete_persistent<T>(p);
		};

		size_type max_size() const
		{
			return PMEMOBJ_MAX_ALLOC_SIZE;
		}

		void construct(pointer p, const_reference val)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(p.get(),
					sizeof (T));
			}
			new((void *)p.get()) T(val);
		}

		template< class U, class... Args >
		void construct( U* p, Args&&... args )
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct((void *)p,
					sizeof (U));
			}
			::new((void *)p) U(std::forward<Args>(args)...);
		}

		void destroy(pointer p)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(p->get(),
					sizeof (T));
			}
			((T*)p->get())->~T();
		}

		template< class U>
		void destroy(U* p)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct((void *)p,
					sizeof (U));
			}
			p->~U();
		}
	};
	template <typename T>
	class pmem_allocator_basic
	{
	public:

		using value_type = T;
		using pointer = persistent_ptr<T>;
		using const_pointer = const persistent_ptr<T>;
		using reference = T&;
		using const_reference = const T&;
		using size_type = std::size_t;
		using difference_type = std::ptrdiff_t;
		using bool_type = bool;

		template <class U>
		struct rebind { using other = pmem_allocator_basic<U>; };

		pmem_allocator_basic() {};
		pmem_allocator_basic(const pmem_allocator_basic&) {};
		template <class U>
		pmem_allocator_basic(const pmem_allocator_basic<U>&) {};
		template <class U>
		pmem_allocator_basic(const pmem_allocator_basic<U>&&) {};


		pmem_allocator_basic& operator=
			(const pmem_allocator_basic& other) = delete;
		~pmem_allocator_basic() {};

//		pointer address(reference r) const { return &r; };
		const_pointer address(const_reference r) const { return &r; };

		bool operator!=(const pmem_allocator_basic& other)
		{
			return !(*this == other);
		};

		bool operator==(const pmem_allocator_basic&)
		{
			return true;
		};

		size_type max_size() const
		{
			return PMEMOBJ_MAX_ALLOC_SIZE;
		}

		pointer allocate(size_type n,
			std::allocator<void>::const_pointer = 0) const
		{
			return make_persistent<T>(sizeof(T) * n);
		};

		void deallocate(pointer p, size_type n)
		{
			delete_persistent<T>(p);
		};

		void construct(pointer p, const_reference val)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(p.get(),
					sizeof (T));
			}
			new((void *)p.get()) T(val);
		}

		template< class U, class... Args >
		void construct( U* p, Args&&... args )
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct((void *)p,
					sizeof (U));
			}
			::new((void *)p) U(std::forward<Args>(args)...);
		}

		void destroy(pointer p)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(p->get(),
					sizeof (T));
			}
			((T*)p->get())->~T();
		}

		template< class U>
		void destroy(U* p)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct((void *)p,
					sizeof (U));
			}
			p->~U();
		}
	};
}

/* use placement new to create user-provided class instance on zeroed memory */
#define PMEM_REGISTER_TYPE(_t, ...) ({\
	void *_type_mem = calloc(1, sizeof (_t));\
	assert(_type_mem != NULL && "failed to register pmem type");\
	memset(_type_mem, 0, sizeof (_t));\
	pmem::register_type<_t>(new (_type_mem) _t(__VA_ARGS__));\
	free(_type_mem);\
})

#endif	/* LIBPMEMOBJPP_H */
