/*
  Copyright (C) 2012-2014  Brazil, Inc.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

#include "grnxx/data_types.hpp"

void test_bool() {
  grnxx::Bool true_object(true);
  grnxx::Bool false_object(false);
  grnxx::Bool na_object((grnxx::NA()));

  assert(true_object.type() == grnxx::BOOL_DATA);
  assert(false_object.type() == grnxx::BOOL_DATA);
  assert(na_object.type() == grnxx::BOOL_DATA);

  assert(true_object.value() == grnxx::Bool::true_value());
  assert(false_object.value() == grnxx::Bool::false_value());
  assert(na_object.value() == grnxx::Bool::na_value());

  assert(true_object.is_true());
  assert(!true_object.is_false());
  assert(!true_object.is_na());

  assert(!false_object.is_true());
  assert(false_object.is_false());
  assert(!false_object.is_na());

  assert(!na_object.is_true());
  assert(!na_object.is_false());
  assert(na_object.is_na());

  assert((!true_object).value() == false_object.value());
  assert((!false_object).value() == true_object.value());
  assert((!na_object).is_na());

  assert((~true_object).value() == false_object.value());
  assert((~false_object).value() == true_object.value());
  assert((~na_object).is_na());

  assert((true_object & true_object).value() == true_object.value());
  assert((true_object & false_object).value() == false_object.value());
  assert((true_object & na_object).is_na());
  assert((false_object & true_object).value() == false_object.value());
  assert((false_object & false_object).value() == false_object.value());
  assert((false_object & na_object).value() == false_object.value());
  assert((na_object & true_object).is_na());
  assert((na_object & false_object).value() == false_object.value());
  assert((na_object & na_object).is_na());

  assert((true_object | true_object).value() == true_object.value());
  assert((true_object | false_object).value() == true_object.value());
  assert((true_object | na_object).value() == true_object.value());
  assert((false_object | true_object).value() == true_object.value());
  assert((false_object | false_object).value() == false_object.value());
  assert((false_object | na_object).is_na());
  assert((na_object | true_object).value() == true_object.value());
  assert((na_object | false_object).is_na());
  assert((na_object | na_object).is_na());

  assert((true_object ^ true_object).value() == false_object.value());
  assert((true_object ^ false_object).value() == true_object.value());
  assert((true_object ^ na_object).is_na());
  assert((false_object ^ true_object).value() == true_object.value());
  assert((false_object ^ false_object).value() == false_object.value());
  assert((false_object ^ na_object).is_na());
  assert((na_object ^ true_object).is_na());
  assert((na_object ^ false_object).is_na());
  assert((na_object ^ na_object).is_na());

  assert((true_object == true_object).value() == true_object.value());
  assert((true_object == false_object).value() == false_object.value());
  assert((true_object == na_object).is_na());
  assert((false_object == true_object).value() == false_object.value());
  assert((false_object == false_object).value() == true_object.value());
  assert((false_object == na_object).is_na());
  assert((na_object == true_object).is_na());
  assert((na_object == false_object).is_na());
  assert((na_object == na_object).is_na());

  assert((true_object != true_object).value() == false_object.value());
  assert((true_object != false_object).value() == true_object.value());
  assert((true_object != na_object).is_na());
  assert((false_object != true_object).value() == true_object.value());
  assert((false_object != false_object).value() == false_object.value());
  assert((false_object != na_object).is_na());
  assert((na_object != true_object).is_na());
  assert((na_object != false_object).is_na());
  assert((na_object != na_object).is_na());

  assert(grnxx::Bool::na().is_na());
}

void test_int() {
  assert(grnxx::Int(0).type() == grnxx::INT_DATA);
  assert(grnxx::Int::min().type() == grnxx::INT_DATA);
  assert(grnxx::Int::max().type() == grnxx::INT_DATA);
  assert(grnxx::Int::na().type() == grnxx::INT_DATA);

  assert(grnxx::Int(0).value() == 0);
  assert(grnxx::Int::min().value() == grnxx::Int::min_value());
  assert(grnxx::Int::max().value() == grnxx::Int::max_value());
  assert(grnxx::Int::na().value() == grnxx::Int::na_value());

  assert(!grnxx::Int(0).is_min());
  assert(grnxx::Int::min().is_min());
  assert(!grnxx::Int::max().is_min());
  assert(!grnxx::Int::na().is_min());

  assert(!grnxx::Int(0).is_max());
  assert(!grnxx::Int::min().is_max());
  assert(grnxx::Int::max().is_max());
  assert(!grnxx::Int::na().is_max());

  assert(!grnxx::Int(0).is_na());
  assert(!grnxx::Int::min().is_na());
  assert(!grnxx::Int::max().is_na());
  assert(grnxx::Int::na().is_na());

  assert((+grnxx::Int(0)).value() == 0);
  assert((+grnxx::Int(1)).value() == 1);
  assert((+grnxx::Int::min()).value() == grnxx::Int::min_value());
  assert((+grnxx::Int::max()).value() == grnxx::Int::max_value());
  assert((+grnxx::Int::na()).is_na());

  assert((-grnxx::Int(0)).value() == 0);
  assert((-grnxx::Int(1)).value() == -1);
  assert((-grnxx::Int::min()).value() == grnxx::Int::max_value());
  assert((-grnxx::Int::max()).value() == grnxx::Int::min_value());
  assert((-grnxx::Int::na()).is_na());

  assert((~grnxx::Int(0)).value() == -1);
  assert((~grnxx::Int(1)).value() == -2);
  assert((~grnxx::Int::min()).value() == (grnxx::Int::max_value() - 1));
  assert((~grnxx::Int::max()).is_na());
  assert((~grnxx::Int::na()).is_na());

  grnxx::Int object(0);

  assert((++object).value() == 1);
  assert((object++).value() == 1);
  assert(object.value() == 2);

  assert((--object).value() == 1);
  assert((object--).value() == 1);
  assert(object.value() == 0);

  object = grnxx::Int::na();

  assert((++object).is_na());
  assert((object++).is_na());
  assert(object.is_na());

  assert((--object).is_na());
  assert((object--).is_na());
  assert(object.is_na());

  assert((grnxx::Int(0) & grnxx::Int(0)).value() == 0);
  assert((grnxx::Int(0) & grnxx::Int(1)).value() == 0);
  assert((grnxx::Int(0) & grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) & grnxx::Int(0)).value() == 0);
  assert((grnxx::Int(1) & grnxx::Int(1)).value() == 1);
  assert((grnxx::Int(1) & grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() & grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() & grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() & grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) | grnxx::Int(0)).value() == 0);
  assert((grnxx::Int(0) | grnxx::Int(1)).value() == 1);
  assert((grnxx::Int(0) | grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) | grnxx::Int(0)).value() == 1);
  assert((grnxx::Int(1) | grnxx::Int(1)).value() == 1);
  assert((grnxx::Int(1) | grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() | grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() | grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() | grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) ^ grnxx::Int(0)).value() == 0);
  assert((grnxx::Int(0) ^ grnxx::Int(1)).value() == 1);
  assert((grnxx::Int(0) ^ grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) ^ grnxx::Int(0)).value() == 1);
  assert((grnxx::Int(1) ^ grnxx::Int(1)).value() == 0);
  assert((grnxx::Int(1) ^ grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int::na()).is_na());

  object = grnxx::Int(3);

  assert((object &= grnxx::Int(1)).value() == 1);
  assert(object.value() == 1);
  assert((object |= grnxx::Int(2)).value() == 3);
  assert(object.value() == 3);
  assert((object ^= grnxx::Int(6)).value() == 5);
  assert(object.value() == 5);

  object = grnxx::Int(0);

  assert((object &= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object &= grnxx::Int(1)).is_na());
  assert(object.is_na());

  object = grnxx::Int(0);

  assert((object |= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object |= grnxx::Int(1)).is_na());
  assert(object.is_na());

  object = grnxx::Int(0);

  assert((object ^= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object ^= grnxx::Int(1)).is_na());
  assert(object.is_na());

  assert((grnxx::Int(1) << grnxx::Int(0)).value() == 1);
  assert((grnxx::Int(1) << grnxx::Int(1)).value() == 2);
  assert((grnxx::Int(1) << grnxx::Int(63)).is_na());
  assert((grnxx::Int(1) << grnxx::Int(64)).is_na());
  assert((grnxx::Int(1) << grnxx::Int(-1)).is_na());
  assert((grnxx::Int(1) << grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() << grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() << grnxx::Int::na()).is_na());

  assert((grnxx::Int(4) >> grnxx::Int(0)).value() == 4);
  assert((grnxx::Int(4) >> grnxx::Int(1)).value() == 2);
  assert((grnxx::Int(4) >> grnxx::Int(63)).value() == 0);
  assert((grnxx::Int(4) >> grnxx::Int(64)).is_na());
  assert((grnxx::Int(4) >> grnxx::Int(-1)).is_na());
  assert((grnxx::Int(4) >> grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() >> grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() >> grnxx::Int::na()).is_na());

  object = grnxx::Int(1);

  assert((object <<= grnxx::Int(3)).value() == 8);
  assert(object.value() == 8);
  assert((object >>= grnxx::Int(2)).value() == 2);
  assert(object.value() == 2);

  object = grnxx::Int(-1);

  assert(object.arithmetic_right_shift(grnxx::Int(0)).value() == -1);
  assert(object.arithmetic_right_shift(grnxx::Int(1)).value() == -1);

  assert(object.logical_right_shift(grnxx::Int(0)).value() == -1);
  assert(object.logical_right_shift(grnxx::Int(1)).is_max());

  assert((grnxx::Int(1) + grnxx::Int(1)).value() == 2);
  assert((grnxx::Int(1) + grnxx::Int::max()).is_na());
  assert((grnxx::Int(1) + grnxx::Int::na()).is_na());
  assert((grnxx::Int(-1) + grnxx::Int(-1)).value() == -2);
  assert((grnxx::Int(-1) + grnxx::Int::min()).is_na());
  assert((grnxx::Int(-1) + grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() + grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() + grnxx::Int::na()).is_na());

  assert((grnxx::Int(1) - grnxx::Int(1)).value() == 0);
  assert((grnxx::Int(1) - grnxx::Int::min()).is_na());
  assert((grnxx::Int(1) - grnxx::Int::na()).is_na());
  assert((grnxx::Int(-1) - grnxx::Int(-1)).value() == 0);
  assert((grnxx::Int(-1) - grnxx::Int::max()).is_na());
  assert((grnxx::Int(-1) - grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() - grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() - grnxx::Int::na()).is_na());

  assert((grnxx::Int(1) * grnxx::Int(0)).value() == 0);
  assert((grnxx::Int(1) * grnxx::Int(2)).value() == 2);
  assert((grnxx::Int(1) * grnxx::Int::min()).is_min());
  assert((grnxx::Int(1) * grnxx::Int::max()).is_max());
  assert((grnxx::Int(1) * grnxx::Int::na()).is_na());
  assert((grnxx::Int(2) * grnxx::Int(0)).value() == 0);
  assert((grnxx::Int(2) * grnxx::Int(2)).value() == 4);
  assert((grnxx::Int(2) * grnxx::Int::min()).is_na());
  assert((grnxx::Int(2) * grnxx::Int::max()).is_na());
  assert((grnxx::Int(2) * grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() * grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() * grnxx::Int(2)).is_na());
  assert((grnxx::Int::na() * grnxx::Int::na()).is_na());

  object = grnxx::Int(1);

  assert((object += grnxx::Int(2)).value() == 3);
  assert(object.value() == 3);
  assert((object -= grnxx::Int(1)).value() == 2);
  assert(object.value() == 2);
  assert((object *= grnxx::Int(4)).value() == 8);
  assert(object.value() == 8);

  object = grnxx::Int(1);

  assert((object += grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object += grnxx::Int(1)).is_na());
  assert(object.is_na());

  object = grnxx::Int(1);

  assert((object -= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object -= grnxx::Int(1)).is_na());
  assert(object.is_na());

  object = grnxx::Int(1);

  assert((object *= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object *= grnxx::Int(1)).is_na());
  assert(object.is_na());

  assert((grnxx::Int(0) / grnxx::Int(0)).is_na());
  assert((grnxx::Int(0) / grnxx::Int(1)).value() == 0);
  assert((grnxx::Int(0) / grnxx::Int(2)).value() == 0);
  assert((grnxx::Int(0) / grnxx::Int::na()).is_na());
  assert((grnxx::Int(2) / grnxx::Int(0)).is_na());
  assert((grnxx::Int(2) / grnxx::Int(1)).value() == 2);
  assert((grnxx::Int(2) / grnxx::Int(2)).value() == 1);
  assert((grnxx::Int(2) / grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) / grnxx::Int(2)).value() == 1);
  assert((grnxx::Int(3) / grnxx::Int(-2)).value() == -1);
  assert((grnxx::Int(-3) / grnxx::Int(2)).value() == -1);
  assert((grnxx::Int(-3) / grnxx::Int(-2)).value() == 1);
  assert((grnxx::Int::min() / grnxx::Int(-1)).is_max());
  assert((grnxx::Int::max() / grnxx::Int(-1)).is_min());
  assert((grnxx::Int::na() / grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() / grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() / grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) % grnxx::Int(0)).is_na());
  assert((grnxx::Int(0) % grnxx::Int(1)).value() == 0);
  assert((grnxx::Int(0) % grnxx::Int(2)).value() == 0);
  assert((grnxx::Int(0) % grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) % grnxx::Int(0)).is_na());
  assert((grnxx::Int(3) % grnxx::Int(1)).value() == 0);
  assert((grnxx::Int(3) % grnxx::Int(2)).value() == 1);
  assert((grnxx::Int(3) % grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) % grnxx::Int(-2)).value() == 1);
  assert((grnxx::Int(-3) % grnxx::Int(2)).value() == -1);
  assert((grnxx::Int(-3) % grnxx::Int(-2)).value() == -1);
  assert((grnxx::Int::na() % grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() % grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() % grnxx::Int::na()).is_na());

  object = grnxx::Int(13);

  assert((object /= grnxx::Int(2)).value() == 6);
  assert(object.value() == 6);
  assert((object %= grnxx::Int(3)).value() == 0);
  assert(object.value() == 0);

  object = grnxx::Int(1);

  assert((object /= grnxx::Int(0)).is_na());
  assert(object.is_na());

  object = grnxx::Int(1);

  assert((object /= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object /= grnxx::Int(1)).is_na());
  assert(object.is_na());

  object = grnxx::Int(1);

  assert((object %= grnxx::Int(0)).is_na());
  assert(object.is_na());

  object = grnxx::Int(1);

  assert((object %= grnxx::Int::na()).is_na());
  assert(object.is_na());
  assert((object %= grnxx::Int(1)).is_na());
  assert(object.is_na());

  assert((grnxx::Int(0) == grnxx::Int(0)).is_true());
  assert((grnxx::Int(0) == grnxx::Int(1)).is_false());
  assert((grnxx::Int(0) == grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) == grnxx::Int(0)).is_false());
  assert((grnxx::Int(1) == grnxx::Int(1)).is_true());
  assert((grnxx::Int(1) == grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() == grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() == grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() == grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) != grnxx::Int(0)).is_false());
  assert((grnxx::Int(0) != grnxx::Int(1)).is_true());
  assert((grnxx::Int(0) != grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) != grnxx::Int(0)).is_true());
  assert((grnxx::Int(1) != grnxx::Int(1)).is_false());
  assert((grnxx::Int(1) != grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() != grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() != grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() != grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) < grnxx::Int(0)).is_false());
  assert((grnxx::Int(0) < grnxx::Int(1)).is_true());
  assert((grnxx::Int(0) < grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) < grnxx::Int(0)).is_false());
  assert((grnxx::Int(1) < grnxx::Int(1)).is_false());
  assert((grnxx::Int(1) < grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() < grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() < grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() < grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) > grnxx::Int(0)).is_false());
  assert((grnxx::Int(0) > grnxx::Int(1)).is_false());
  assert((grnxx::Int(0) > grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) > grnxx::Int(0)).is_true());
  assert((grnxx::Int(1) > grnxx::Int(1)).is_false());
  assert((grnxx::Int(1) > grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() > grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() > grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() > grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) <= grnxx::Int(0)).is_true());
  assert((grnxx::Int(0) <= grnxx::Int(1)).is_true());
  assert((grnxx::Int(0) <= grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) <= grnxx::Int(0)).is_false());
  assert((grnxx::Int(1) <= grnxx::Int(1)).is_true());
  assert((grnxx::Int(1) <= grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() <= grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() <= grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() <= grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) >= grnxx::Int(0)).is_true());
  assert((grnxx::Int(0) >= grnxx::Int(1)).is_false());
  assert((grnxx::Int(0) >= grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) >= grnxx::Int(0)).is_true());
  assert((grnxx::Int(1) >= grnxx::Int(1)).is_true());
  assert((grnxx::Int(1) >= grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() >= grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() >= grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() >= grnxx::Int::na()).is_na());
}

void test_float() {
  assert(grnxx::Float(0.0).type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::min().type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::max().type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::normal_min().type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::subnormal_min().type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::max().type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::infinity().type() == grnxx::FLOAT_DATA);
  assert(grnxx::Float::na().type() == grnxx::FLOAT_DATA);

  assert(grnxx::Float(0.0).value() == 0.0);
  assert(grnxx::Float::min().value() == grnxx::Float::min_value());
  assert(grnxx::Float::max().value() == grnxx::Float::max_value());
  assert(grnxx::Float::normal_min().value() ==
         grnxx::Float::normal_min_value());
  assert(grnxx::Float::subnormal_min().value() ==
         grnxx::Float::subnormal_min_value());
  assert(grnxx::Float::infinity().value() == grnxx::Float::infinity_value());
  assert(std::isnan(grnxx::Float::na().value()));

  assert(!grnxx::Float(0.0).is_min());
  assert(grnxx::Float::min().is_min());
  assert(!grnxx::Float::max().is_min());
  assert(!grnxx::Float::infinity().is_min());
  assert(!grnxx::Float::na().is_min());

  assert(!grnxx::Float(0.0).is_max());
  assert(!grnxx::Float::min().is_max());
  assert(grnxx::Float::max().is_max());
  assert(!grnxx::Float::infinity().is_max());
  assert(!grnxx::Float::na().is_max());

  assert(grnxx::Float(0.0).is_finite());
  assert(grnxx::Float::min().is_finite());
  assert(grnxx::Float::max().is_finite());
  assert(!grnxx::Float::infinity().is_finite());
  assert(!grnxx::Float::na().is_finite());

  assert(!grnxx::Float(0.0).is_infinite());
  assert(!grnxx::Float::min().is_infinite());
  assert(!grnxx::Float::max().is_infinite());
  assert(grnxx::Float::infinity().is_infinite());
  assert(!grnxx::Float::na().is_infinite());

  assert(!grnxx::Float(0.0).is_na());
  assert(!grnxx::Float::min().is_na());
  assert(!grnxx::Float::max().is_na());
  assert(!grnxx::Float::infinity().is_na());
  assert(grnxx::Float::na().is_na());

  assert((+grnxx::Float(0.0)).value() == 0.0);
  assert((+grnxx::Float(1.0)).value() == 1.0);
  assert((+grnxx::Float::min()).value() == grnxx::Float::min_value());
  assert((+grnxx::Float::max()).value() == grnxx::Float::max_value());
  assert((+grnxx::Float::infinity()).value() ==
         grnxx::Float::infinity_value());
  assert((+grnxx::Float::na()).is_na());

  assert((-grnxx::Float(0.0)).value() == 0.0);
  assert((-grnxx::Float(1.0)).value() == -1.0);
  assert((-grnxx::Float::min()).value() == grnxx::Float::max_value());
  assert((-grnxx::Float::max()).value() == grnxx::Float::min_value());
  assert((-grnxx::Float::infinity()).value() ==
         -grnxx::Float::infinity_value());
  assert((-grnxx::Float::na()).is_na());

  assert((grnxx::Float(1.0) + grnxx::Float(1.0)).value() == 2.0);
  assert((grnxx::Float::max() + grnxx::Float::max()).is_infinite());
  assert((grnxx::Float::infinity() + grnxx::Float::min()).is_infinite());
  assert((grnxx::Float::infinity() + -grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(1.0) + grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() + grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() + grnxx::Float::na()).is_na());

  assert((grnxx::Float(1.0) - grnxx::Float(1.0)).value() == 0.0);
  assert((grnxx::Float::max() - -grnxx::Float::max()).is_infinite());
  assert((grnxx::Float::infinity() - grnxx::Float::max()).is_infinite());
  assert((grnxx::Float::infinity() - grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(1.0) - grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() - grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() - grnxx::Float::na()).is_na());

  assert((grnxx::Float(2.0) * grnxx::Float(0.5)).value() == 1.0);
  assert((grnxx::Float::max() * grnxx::Float::max()).is_infinite());
  assert((grnxx::Float::infinity() * grnxx::Float::subnormal_min()).value()
         == grnxx::Float::infinity_value());
  assert((grnxx::Float::infinity() * grnxx::Float(0.0)).is_na());
  assert((grnxx::Float(1.0) * grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() * grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() * grnxx::Float::na()).is_na());

  grnxx::Float object(1.0);

  assert((object += grnxx::Float(2.0)).value() == 3.0);
  assert(object.value() == 3.0);
  assert((object -= grnxx::Float(1.0)).value() == 2.0);
  assert(object.value() == 2.0);
  assert((object *= grnxx::Float(4.0)).value() == 8.0);
  assert(object.value() == 8.0);

  object = grnxx::Float(1.0);

  assert((object += grnxx::Float::na()).is_na());
  assert(object.is_na());
  assert((object += grnxx::Float(1.0)).is_na());
  assert(object.is_na());

  object = grnxx::Float(1.0);

  assert((object -= grnxx::Float::na()).is_na());
  assert(object.is_na());
  assert((object -= grnxx::Float(1.0)).is_na());
  assert(object.is_na());

  object = grnxx::Float(1.0);

  assert((object *= grnxx::Float::na()).is_na());
  assert(object.is_na());
  assert((object *= grnxx::Float(1.0)).is_na());
  assert(object.is_na());

  assert((grnxx::Float(1.0) / grnxx::Float(2.0)).value() == 0.5);
  assert((grnxx::Float(1.0) / grnxx::Float(0.0)).is_infinite());
  assert((grnxx::Float(1.0) / grnxx::Float::infinity()).value() == 0.0);
  assert((grnxx::Float::max() / grnxx::Float::subnormal_min()).is_infinite());
  assert((grnxx::Float::infinity() / grnxx::Float::max()).is_infinite());
  assert((grnxx::Float::infinity() / grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(0.0) / grnxx::Float(0.0)).is_na());
  assert((grnxx::Float(1.0) / grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() / grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() / grnxx::Float::na()).is_na());

  assert((grnxx::Float(1.0) % grnxx::Float(2.0)).value() == 1.0);
  assert((grnxx::Float(1.0) % grnxx::Float(-2.0)).value() == 1.0);
  assert((grnxx::Float(-1.0) % grnxx::Float(2.0)).value() == -1.0);
  assert((grnxx::Float(-1.0) % grnxx::Float(-2.0)).value() == -1.0);
  assert((grnxx::Float(1.0) % grnxx::Float::infinity()).value() == 1.0);
  assert((grnxx::Float::infinity() % grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::infinity() % grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(0.0) % grnxx::Float(0.0)).is_na());
  assert((grnxx::Float(1.0) % grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() % grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() % grnxx::Float::na()).is_na());

  object = grnxx::Float(13.0);

  assert((object /= grnxx::Float(2.0)).value() == 6.5);
  assert(object.value() == 6.5);
  assert((object %= grnxx::Float(3.0)).value() == 0.5);
  assert(object.value() == 0.5);

  object = grnxx::Float(1.0);

  assert((object /= grnxx::Float::na()).is_na());
  assert(object.is_na());
  assert((object /= grnxx::Float(1.0)).is_na());
  assert(object.is_na());

  object = grnxx::Float(1.0);

  assert((object %= grnxx::Float::na()).is_na());
  assert(object.is_na());
  assert((object %= grnxx::Float(1.0)).is_na());
  assert(object.is_na());

  assert((grnxx::Float::min() == grnxx::Float::min()).is_true());
  assert((grnxx::Float::min() == grnxx::Float::max()).is_false());
  assert((grnxx::Float::min() == grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::min() == grnxx::Float::na()).is_na());
  assert((grnxx::Float::max() == grnxx::Float::min()).is_false());
  assert((grnxx::Float::max() == grnxx::Float::max()).is_true());
  assert((grnxx::Float::max() == grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::max() == grnxx::Float::na()).is_na());
  assert((grnxx::Float::infinity() == grnxx::Float::min()).is_false());
  assert((grnxx::Float::infinity() == grnxx::Float::max()).is_false());
  assert((grnxx::Float::infinity() == grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::infinity() == grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() == grnxx::Float::min()).is_na());
  assert((grnxx::Float::na() == grnxx::Float::max()).is_na());
  assert((grnxx::Float::na() == grnxx::Float::infinity()).is_na());
  assert((grnxx::Float::na() == grnxx::Float::na()).is_na());

  assert((grnxx::Float::min() != grnxx::Float::min()).is_false());
  assert((grnxx::Float::min() != grnxx::Float::max()).is_true());
  assert((grnxx::Float::min() != grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::min() != grnxx::Float::na()).is_na());
  assert((grnxx::Float::max() != grnxx::Float::min()).is_true());
  assert((grnxx::Float::max() != grnxx::Float::max()).is_false());
  assert((grnxx::Float::max() != grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::max() != grnxx::Float::na()).is_na());
  assert((grnxx::Float::infinity() != grnxx::Float::min()).is_true());
  assert((grnxx::Float::infinity() != grnxx::Float::max()).is_true());
  assert((grnxx::Float::infinity() != grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::infinity() != grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() != grnxx::Float::min()).is_na());
  assert((grnxx::Float::na() != grnxx::Float::max()).is_na());
  assert((grnxx::Float::na() != grnxx::Float::infinity()).is_na());
  assert((grnxx::Float::na() != grnxx::Float::na()).is_na());

  assert((grnxx::Float::min() < grnxx::Float::min()).is_false());
  assert((grnxx::Float::min() < grnxx::Float::max()).is_true());
  assert((grnxx::Float::min() < grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::min() < grnxx::Float::na()).is_na());
  assert((grnxx::Float::max() < grnxx::Float::min()).is_false());
  assert((grnxx::Float::max() < grnxx::Float::max()).is_false());
  assert((grnxx::Float::max() < grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::max() < grnxx::Float::na()).is_na());
  assert((grnxx::Float::infinity() < grnxx::Float::min()).is_false());
  assert((grnxx::Float::infinity() < grnxx::Float::max()).is_false());
  assert((grnxx::Float::infinity() < grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::infinity() < grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() < grnxx::Float::min()).is_na());
  assert((grnxx::Float::na() < grnxx::Float::max()).is_na());
  assert((grnxx::Float::na() < grnxx::Float::infinity()).is_na());
  assert((grnxx::Float::na() < grnxx::Float::na()).is_na());

  assert((grnxx::Float::min() > grnxx::Float::min()).is_false());
  assert((grnxx::Float::min() > grnxx::Float::max()).is_false());
  assert((grnxx::Float::min() > grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::min() > grnxx::Float::na()).is_na());
  assert((grnxx::Float::max() > grnxx::Float::min()).is_true());
  assert((grnxx::Float::max() > grnxx::Float::max()).is_false());
  assert((grnxx::Float::max() > grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::max() > grnxx::Float::na()).is_na());
  assert((grnxx::Float::infinity() > grnxx::Float::min()).is_true());
  assert((grnxx::Float::infinity() > grnxx::Float::max()).is_true());
  assert((grnxx::Float::infinity() > grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::infinity() > grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() > grnxx::Float::min()).is_na());
  assert((grnxx::Float::na() > grnxx::Float::max()).is_na());
  assert((grnxx::Float::na() > grnxx::Float::infinity()).is_na());
  assert((grnxx::Float::na() > grnxx::Float::na()).is_na());

  assert((grnxx::Float::min() <= grnxx::Float::min()).is_true());
  assert((grnxx::Float::min() <= grnxx::Float::max()).is_true());
  assert((grnxx::Float::min() <= grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::min() <= grnxx::Float::na()).is_na());
  assert((grnxx::Float::max() <= grnxx::Float::min()).is_false());
  assert((grnxx::Float::max() <= grnxx::Float::max()).is_true());
  assert((grnxx::Float::max() <= grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::max() <= grnxx::Float::na()).is_na());
  assert((grnxx::Float::infinity() <= grnxx::Float::min()).is_false());
  assert((grnxx::Float::infinity() <= grnxx::Float::max()).is_false());
  assert((grnxx::Float::infinity() <= grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::infinity() <= grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() <= grnxx::Float::min()).is_na());
  assert((grnxx::Float::na() <= grnxx::Float::max()).is_na());
  assert((grnxx::Float::na() <= grnxx::Float::infinity()).is_na());
  assert((grnxx::Float::na() <= grnxx::Float::na()).is_na());

  assert((grnxx::Float::min() >= grnxx::Float::min()).is_true());
  assert((grnxx::Float::min() >= grnxx::Float::max()).is_false());
  assert((grnxx::Float::min() >= grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::min() >= grnxx::Float::na()).is_na());
  assert((grnxx::Float::max() >= grnxx::Float::min()).is_true());
  assert((grnxx::Float::max() >= grnxx::Float::max()).is_true());
  assert((grnxx::Float::max() >= grnxx::Float::infinity()).is_false());
  assert((grnxx::Float::max() >= grnxx::Float::na()).is_na());
  assert((grnxx::Float::infinity() >= grnxx::Float::min()).is_true());
  assert((grnxx::Float::infinity() >= grnxx::Float::max()).is_true());
  assert((grnxx::Float::infinity() >= grnxx::Float::infinity()).is_true());
  assert((grnxx::Float::infinity() >= grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() >= grnxx::Float::min()).is_na());
  assert((grnxx::Float::na() >= grnxx::Float::max()).is_na());
  assert((grnxx::Float::na() >= grnxx::Float::infinity()).is_na());
  assert((grnxx::Float::na() >= grnxx::Float::na()).is_na());

  assert((grnxx::Float(0.0).next_toward(grnxx::Float::max())).value() ==
         grnxx::Float::subnormal_min_value());
  assert((grnxx::Float(0.0).next_toward(-grnxx::Float::max())).value() ==
         -grnxx::Float::subnormal_min_value());
  assert((grnxx::Float(0.0).next_toward(grnxx::Float::infinity())).value() ==
         grnxx::Float::subnormal_min_value());
  assert((grnxx::Float(0.0).next_toward(-grnxx::Float::infinity())).value() ==
         -grnxx::Float::subnormal_min_value());
  assert((grnxx::Float::infinity().next_toward(grnxx::Float(0.0))).value()
         == grnxx::Float::max_value());
  assert((-grnxx::Float::infinity().next_toward(grnxx::Float(0.0))).value()
         == grnxx::Float::min_value());
  assert((grnxx::Float(0.0).next_toward(grnxx::Float::na())).is_na());
  assert((grnxx::Float::na().next_toward(grnxx::Float(0.0))).is_na());
  assert((grnxx::Float::na().next_toward(grnxx::Float::na())).is_na());
}

void test_geo_point() {
  grnxx::GeoPoint zero(grnxx::Int(0), grnxx::Int(0));
  grnxx::GeoPoint north_pole(grnxx::Float(90.0), grnxx::Float(100.0));
  grnxx::GeoPoint south_pole(grnxx::Float(-90.0), grnxx::Float(100.0));
  grnxx::GeoPoint date_line(grnxx::Float(0.0), grnxx::Float(180.0));
  grnxx::GeoPoint na(grnxx::Int::na(), grnxx::Int::na());

  assert(zero.type() == grnxx::GEO_POINT_DATA);
  assert(north_pole.type() == grnxx::GEO_POINT_DATA);
  assert(south_pole.type() == grnxx::GEO_POINT_DATA);
  assert(date_line.type() == grnxx::GEO_POINT_DATA);
  assert(na.type() == grnxx::GEO_POINT_DATA);

  assert(zero.latitude() == 0);
  assert(date_line.latitude() == 0);
  assert(na.latitude() == grnxx::GeoPoint::na_latitude());

  assert(zero.longitude() == 0);
  assert(north_pole.longitude() == 0);
  assert(south_pole.longitude() == 0);
  assert(na.longitude() == grnxx::GeoPoint::na_longitude());

  assert(zero.latitude_in_milliseconds().value() == 0);
  assert(date_line.latitude_in_milliseconds().value() == 0);
  assert(na.latitude_in_milliseconds().is_na());

  assert(zero.longitude_in_milliseconds().value() == 0);
  assert(north_pole.longitude_in_milliseconds().value() == 0);
  assert(south_pole.longitude_in_milliseconds().value() == 0);
  assert(na.longitude_in_milliseconds().is_na());

  assert(zero.latitude_in_degrees().value() == 0.0);
  assert(north_pole.latitude_in_degrees().value() == 90.0);
  assert(south_pole.latitude_in_degrees().value() == -90.0);
  assert(date_line.latitude_in_degrees().value() == 0.0);
  assert(na.latitude_in_degrees().is_na());

  assert(zero.longitude_in_degrees().value() == 0.0);
  assert(north_pole.longitude_in_degrees().value() == 0.0);
  assert(south_pole.longitude_in_degrees().value() == 0.0);
  assert(date_line.longitude_in_degrees().value() == -180.0);
  assert(na.longitude_in_degrees().is_na());

  assert((zero == zero).is_true());
  assert((zero == north_pole).is_false());
  assert((zero == south_pole).is_false());
  assert((zero == date_line).is_false());
  assert((zero == na).is_na());
  assert((north_pole == north_pole).is_true());
  assert((north_pole == south_pole).is_false());
  assert((north_pole == date_line).is_false());
  assert((north_pole == na).is_na());
  assert((south_pole == south_pole).is_true());
  assert((south_pole == date_line).is_false());
  assert((south_pole == na).is_na());
  assert((date_line == date_line).is_true());
  assert((date_line == na).is_na());
  assert((na == na).is_na());

  assert((zero != zero).is_false());
  assert((zero != north_pole).is_true());
  assert((zero != south_pole).is_true());
  assert((zero != date_line).is_true());
  assert((zero != na).is_na());
  assert((north_pole != north_pole).is_false());
  assert((north_pole != south_pole).is_true());
  assert((north_pole != date_line).is_true());
  assert((north_pole != na).is_na());
  assert((south_pole != south_pole).is_false());
  assert((south_pole != date_line).is_true());
  assert((south_pole != na).is_na());
  assert((date_line != date_line).is_false());
  assert((date_line != na).is_na());
  assert((na != na).is_na());
}

void test_text() {
  grnxx::Text ab("ab");
  grnxx::Text abc("abc", 3);
  grnxx::Text bc(grnxx::String("bc"));
  grnxx::Text empty = grnxx::Text::empty();
  grnxx::Text na = grnxx::Text::na();

  assert(ab.type() == grnxx::TEXT_DATA);
  assert(abc.type() == grnxx::TEXT_DATA);
  assert(bc.type() == grnxx::TEXT_DATA);
  assert(empty.type() == grnxx::TEXT_DATA);
  assert(na.type() == grnxx::TEXT_DATA);

  assert(std::strcmp(ab.data(), "ab") == 0);
  assert(std::strcmp(abc.data(), "abc") == 0);
  assert(std::strcmp(bc.data(), "bc") == 0);

  assert(ab.size().value() == 2);
  assert(abc.size().value() == 3);
  assert(bc.size().value() == 2);
  assert(empty.size().value() == 0);
  assert(na.size().is_na());

  assert(!ab.is_empty());
  assert(!abc.is_empty());
  assert(!bc.is_empty());
  assert(empty.is_empty());
  assert(!na.is_empty());

  assert(!ab.is_na());
  assert(!abc.is_na());
  assert(!bc.is_na());
  assert(!empty.is_na());
  assert(na.is_na());

  assert((ab == ab).is_true());
  assert((ab == abc).is_false());
  assert((ab == bc).is_false());
  assert((ab == empty).is_false());
  assert((ab == na).is_na());
  assert((abc == abc).is_true());
  assert((abc == bc).is_false());
  assert((abc == empty).is_false());
  assert((abc == na).is_na());
  assert((bc == bc).is_true());
  assert((bc == empty).is_false());
  assert((bc == na).is_na());
  assert((empty == empty).is_true());
  assert((empty == na).is_na());
  assert((na == na).is_na());

  assert((ab != ab).is_false());
  assert((ab != abc).is_true());
  assert((ab != bc).is_true());
  assert((ab != empty).is_true());
  assert((ab != na).is_na());
  assert((abc != abc).is_false());
  assert((abc != bc).is_true());
  assert((abc != empty).is_true());
  assert((abc != na).is_na());
  assert((bc != bc).is_false());
  assert((bc != empty).is_true());
  assert((bc != na).is_na());
  assert((empty != empty).is_false());
  assert((empty != na).is_na());
  assert((na != na).is_na());

  assert((ab < ab).is_false());
  assert((ab < abc).is_true());
  assert((ab < bc).is_true());
  assert((ab < empty).is_false());
  assert((ab < na).is_na());
  assert((abc < abc).is_false());
  assert((abc < bc).is_true());
  assert((abc < empty).is_false());
  assert((abc < na).is_na());
  assert((bc < bc).is_false());
  assert((bc < empty).is_false());
  assert((bc < na).is_na());
  assert((empty < empty).is_false());
  assert((empty < na).is_na());
  assert((na < na).is_na());

  assert((ab > ab).is_false());
  assert((ab > abc).is_false());
  assert((ab > bc).is_false());
  assert((ab > empty).is_true());
  assert((ab > na).is_na());
  assert((abc > abc).is_false());
  assert((abc > bc).is_false());
  assert((abc > empty).is_true());
  assert((abc > na).is_na());
  assert((bc > bc).is_false());
  assert((bc > empty).is_true());
  assert((bc > na).is_na());
  assert((empty > empty).is_false());
  assert((empty > na).is_na());
  assert((na > na).is_na());

  assert((ab <= ab).is_true());
  assert((ab <= abc).is_true());
  assert((ab <= bc).is_true());
  assert((ab <= empty).is_false());
  assert((ab <= na).is_na());
  assert((abc <= abc).is_true());
  assert((abc <= bc).is_true());
  assert((abc <= empty).is_false());
  assert((abc <= na).is_na());
  assert((bc <= bc).is_true());
  assert((bc <= empty).is_false());
  assert((bc <= na).is_na());
  assert((empty <= empty).is_true());
  assert((empty <= na).is_na());
  assert((na <= na).is_na());

  assert((ab >= ab).is_true());
  assert((ab >= abc).is_false());
  assert((ab >= bc).is_false());
  assert((ab >= empty).is_true());
  assert((ab >= na).is_na());
  assert((abc >= abc).is_true());
  assert((abc >= bc).is_false());
  assert((abc >= empty).is_true());
  assert((abc >= na).is_na());
  assert((bc >= bc).is_true());
  assert((bc >= empty).is_true());
  assert((bc >= na).is_na());
  assert((empty >= empty).is_true());
  assert((empty >= na).is_na());
  assert((na >= na).is_na());

  assert((ab.starts_with(ab)).is_true());
  assert((ab.starts_with(abc)).is_false());
  assert((ab.starts_with(bc)).is_false());
  assert((ab.starts_with(empty)).is_true());
  assert((ab.starts_with(na)).is_na());
  assert((abc.starts_with(ab)).is_true());
  assert((abc.starts_with(abc)).is_true());
  assert((abc.starts_with(bc)).is_false());
  assert((abc.starts_with(empty)).is_true());
  assert((abc.starts_with(na)).is_na());
  assert((bc.starts_with(ab)).is_false());
  assert((bc.starts_with(abc)).is_false());
  assert((bc.starts_with(bc)).is_true());
  assert((bc.starts_with(empty)).is_true());
  assert((bc.starts_with(na)).is_na());
  assert((empty.starts_with(ab)).is_false());
  assert((empty.starts_with(abc)).is_false());
  assert((empty.starts_with(bc)).is_false());
  assert((empty.starts_with(empty)).is_true());
  assert((empty.starts_with(na)).is_na());
  assert((na.starts_with(ab)).is_na());
  assert((na.starts_with(abc)).is_na());
  assert((na.starts_with(bc)).is_na());
  assert((na.starts_with(empty)).is_na());
  assert((na.starts_with(na)).is_na());

  assert((ab.ends_with(ab)).is_true());
  assert((ab.ends_with(abc)).is_false());
  assert((ab.ends_with(bc)).is_false());
  assert((ab.ends_with(empty)).is_true());
  assert((ab.ends_with(na)).is_na());
  assert((abc.ends_with(ab)).is_false());
  assert((abc.ends_with(abc)).is_true());
  assert((abc.ends_with(bc)).is_true());
  assert((abc.ends_with(empty)).is_true());
  assert((abc.ends_with(na)).is_na());
  assert((bc.ends_with(ab)).is_false());
  assert((bc.ends_with(abc)).is_false());
  assert((bc.ends_with(bc)).is_true());
  assert((bc.ends_with(empty)).is_true());
  assert((bc.ends_with(na)).is_na());
  assert((empty.ends_with(ab)).is_false());
  assert((empty.ends_with(abc)).is_false());
  assert((empty.ends_with(bc)).is_false());
  assert((empty.ends_with(empty)).is_true());
  assert((empty.ends_with(na)).is_na());
  assert((na.ends_with(ab)).is_na());
  assert((na.ends_with(abc)).is_na());
  assert((na.ends_with(bc)).is_na());
  assert((na.ends_with(empty)).is_na());
  assert((na.ends_with(na)).is_na());
}

void test_bool_vector() {
  grnxx::Bool data[] = {
    grnxx::Bool(true),
    grnxx::Bool(false),
    grnxx::Bool(true),
    grnxx::Bool::na(),
  };

  grnxx::BoolVector tft = grnxx::BoolVector(data, 3);
  grnxx::BoolVector ftn = grnxx::BoolVector(data + 1, 3);
  grnxx::BoolVector empty = grnxx::BoolVector::empty();
  grnxx::BoolVector na = grnxx::BoolVector::na();

  assert(tft.type() == grnxx::BOOL_VECTOR_DATA);
  assert(ftn.type() == grnxx::BOOL_VECTOR_DATA);
  assert(empty.type() == grnxx::BOOL_VECTOR_DATA);
  assert(na.type() == grnxx::BOOL_VECTOR_DATA);

  assert(tft[0].is_true());
  assert(tft[1].is_false());
  assert(tft[2].is_true());

  assert(ftn[0].is_false());
  assert(ftn[1].is_true());
  assert(ftn[2].is_na());

  assert(tft.size().value() == 3);
  assert(ftn.size().value() == 3);
  assert(empty.size().value() == 0);
  assert(na.size().is_na());

  assert(!tft.is_empty());
  assert(!tft.is_empty());
  assert(empty.is_empty());
  assert(!na.is_empty());

  assert(!tft.is_na());
  assert(!ftn.is_na());
  assert(!empty.is_na());
  assert(na.is_na());

  assert((tft == tft).is_true());
  assert((tft == ftn).is_false());
  assert((tft == empty).is_false());
  assert((tft == na).is_na());
  assert((ftn == ftn).is_true());
  assert((ftn == empty).is_false());
  assert((ftn == na).is_na());
  assert((empty == empty).is_true());
  assert((empty == na).is_na());
  assert((na == na).is_na());

  assert((tft != tft).is_false());
  assert((tft != ftn).is_true());
  assert((tft != empty).is_true());
  assert((tft != na).is_na());
  assert((ftn != ftn).is_false());
  assert((ftn != empty).is_true());
  assert((ftn != na).is_na());
  assert((empty != empty).is_false());
  assert((empty != na).is_na());
  assert((na != na).is_na());
}

void test_int_vector() {
  grnxx::Int data[] = {
    grnxx::Int(1),
    grnxx::Int(2),
    grnxx::Int(3),
    grnxx::Int::na(),
  };

  grnxx::IntVector abc = grnxx::IntVector(data, 3);
  grnxx::IntVector bcn = grnxx::IntVector(data + 1, 3);
  grnxx::IntVector empty = grnxx::IntVector::empty();
  grnxx::IntVector na = grnxx::IntVector::na();

  assert(abc.type() == grnxx::INT_VECTOR_DATA);
  assert(bcn.type() == grnxx::INT_VECTOR_DATA);
  assert(empty.type() == grnxx::INT_VECTOR_DATA);
  assert(na.type() == grnxx::INT_VECTOR_DATA);

  assert(abc[0].value() == 1);
  assert(abc[1].value() == 2);
  assert(abc[2].value() == 3);

  assert(bcn[0].value() == 2);
  assert(bcn[1].value() == 3);
  assert(bcn[2].is_na());

  assert(abc.size().value() == 3);
  assert(bcn.size().value() == 3);
  assert(empty.size().value() == 0);
  assert(na.size().is_na());

  assert(!abc.is_empty());
  assert(!abc.is_empty());
  assert(empty.is_empty());
  assert(!na.is_empty());

  assert(!abc.is_na());
  assert(!bcn.is_na());
  assert(!empty.is_na());
  assert(na.is_na());

  assert((abc == abc).is_true());
  assert((abc == bcn).is_false());
  assert((abc == empty).is_false());
  assert((abc == na).is_na());
  assert((bcn == bcn).is_true());
  assert((bcn == empty).is_false());
  assert((bcn == na).is_na());
  assert((empty == empty).is_true());
  assert((empty == na).is_na());
  assert((na == na).is_na());

  assert((abc != abc).is_false());
  assert((abc != bcn).is_true());
  assert((abc != empty).is_true());
  assert((abc != na).is_na());
  assert((bcn != bcn).is_false());
  assert((bcn != empty).is_true());
  assert((bcn != na).is_na());
  assert((empty != empty).is_false());
  assert((empty != na).is_na());
  assert((na != na).is_na());
}

int main() {
  test_bool();
  test_int();
  test_float();
  test_geo_point();
  test_text();
  test_bool_vector();
  test_int_vector();
  return 0;
}
