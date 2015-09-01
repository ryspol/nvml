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
#include <thread>
#include <mutex>
#include <map>
#include <list>
#include <type_traits>

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
	static void register_type(T *ptr) {
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

	template<typename T>
	class persistent_ptr
	{
		template<typename A>
		friend void delete_persistent(persistent_ptr<A> ptr);

		template<typename A>
		friend void make_persistent_atomic(base_pool &pop,
			persistent_ptr<A> &ptr);

		template<typename A>
		friend void delete_persistent_atomic(persistent_ptr<A> &ptr);

		template<typename V>
		friend class persistent_ptr;

		template<typename I, typename UI>
		friend class piterator;
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

		inline persistent_ptr& operator=(persistent_ptr const &rhs)
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK)
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));

			oid = rhs.oid;

			return *this;
		}

		template<typename V>
		inline persistent_ptr& operator=(persistent_ptr<V> const &rhs)
		{
			static_assert(std::is_assignable<T, V>::value,
				"assigning incompatible persistent types");

			if (pmemobj_tx_stage() == TX_STAGE_WORK)
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));

			oid = rhs.oid;

			return *this;
		}

		inline T* get() const
		{
			uintptr_t *d = (uintptr_t *)pmemobj_direct(oid);
			/* do this once per run (add runid) */
			if (d && std::is_polymorphic<T>::value) {
				fix_vtables();
			}

			return (T*)d;
		}

		/* XXX: operator[] */
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
				pmemobj_tx_end();
				throw;
			}

			if (pmemobj_tx_process() != 0) {
				pmemobj_tx_end();
				throw transaction_error("failed to process"
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
		void *operator new(std::size_t size);
		void operator delete(void *ptr, std::size_t size);
	public:
		p(T _val) : val(_val)
		{
		}

		p()
		{
		}

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
	private:
		T val;
	};

	template<typename T>
	persistent_ptr<T> make_persistent()
	{
		persistent_ptr<T> ptr = pmemobj_tx_alloc(sizeof (T),
			type_num<T>());

		new (ptr.get()) T();

		return ptr;
	}

	template<typename T, typename... Args>
	persistent_ptr<T> make_persistent(Args && ... args)
	{
		if (pmemobj_tx_stage() != TX_STAGE_WORK)
			throw transaction_scope_error("refusing to allocate"
				"memory outside of transaction scope");

		persistent_ptr<T> ptr = pmemobj_tx_alloc(sizeof (T),
			type_num<T>());

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
