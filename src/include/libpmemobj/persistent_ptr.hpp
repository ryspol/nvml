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

/*
 * persistent_ptr.hpp -- persistent smart pointer
 */

#ifndef PERSISTENT_PTR_HPP
#define PERSISTENT_PTR_HPP

#include <memory>
#include "libpmemobj.h"

#include "libpmemobj/detail/specialization.hpp"

namespace nvml
{

namespace pmem
{
	template<typename T> class persistent_ptr
	{
	public:
		typedef typename nvml::detail::sp_element<T>::type element_type;

		persistent_ptr() noexcept : oid(OID_NULL)
		{
			static_assert(std::is_pod<element_type>::value, "");
		}

		persistent_ptr(std::nullptr_t) noexcept : oid(OID_NULL)
		{
			static_assert(std::is_pod<element_type>::value, "");
		}

		persistent_ptr(PMEMoid o) noexcept : oid(o)
		{
			static_assert(std::is_pod<element_type>::value, "");
		}

		template <typename Y>
		persistent_ptr(persistent_ptr<Y> const & r)
			noexcept : oid(r.oid)
		{
			static_assert(std::is_pod<element_type>::value, "");

			static_assert(std::is_convertible<Y, T>::value,
				"assignment of inconvertible types");
		}

		persistent_ptr(persistent_ptr && r) noexcept : oid(r.oid)
		{
			static_assert(std::is_pod<element_type>::value, "");

		}

		template<typename Y>
		persistent_ptr & operator=(persistent_ptr<Y> && r) noexcept
		{
			static_assert(std::is_convertible<Y, T>::value,
				"assignment of inconvertible types");

			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}

			r.swap(*this);

			return *this;
		}

		persistent_ptr & operator=(persistent_ptr const & r) noexcept
		{
			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}

			r.swap(*this);

			return *this;
		}

		template<typename Y>
		persistent_ptr & operator=(persistent_ptr<Y> const & r) noexcept
		{
			static_assert(std::is_convertible<Y, T>::value,
				"assignment of inconvertible types");

			if (pmemobj_tx_stage() == TX_STAGE_WORK) {
				pmemobj_tx_add_range_direct(this,
					sizeof (*this));
			}

			r.swap(*this);

			return *this;
		}

		typename nvml::detail::sp_dereference<T>::type
			operator* () const noexcept
		{
			return *get();
		}

		typename nvml::detail::sp_member_access<T>::type
			operator-> () const noexcept
		{
			return get();
		}

		typename nvml::detail::sp_array_access<T>::type
			operator[] (std::ptrdiff_t i) const noexcept
		{
			static_assert(i >= 0 &&
				(i < nvml::detail::sp_extent<T>::value ||
				nvml::detail::sp_extent<T>::value == 0),
				"persistent array index out of bounds");

			return get()[i];
		}

		element_type * get() const noexcept
		{
			return (element_type *)pmemobj_direct(oid);
		}

		void swap(persistent_ptr &other) noexcept
		{
			std::swap(oid, other.oid);
		}

		typedef element_type*
			(persistent_ptr<T>::*unspecified_bool_type)() const;

		operator unspecified_bool_type() const
		{
			return OID_IS_NULL(oid) ? 0 : &persistent_ptr<T>::get;
		}

		explicit operator bool() const
		{
			return get() != nullptr;
		}
	private:
		PMEMoid oid;
	};

} /* namespace pmem */

} /* namespace nvml */

#endif /* PERSISTENT_PTR_HPP */
