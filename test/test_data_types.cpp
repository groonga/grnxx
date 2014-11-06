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

  assert(static_cast<bool>(true_object));
  assert(!static_cast<bool>(false_object));
  assert(!static_cast<bool>(na_object));

  assert(!true_object == false_object);
  assert(!false_object == true_object);
  assert((!na_object).is_na());

  assert(~true_object == false_object);
  assert(~false_object == true_object);
  assert((~na_object).is_na());

  assert((true_object & true_object) == true_object);
  assert((true_object & false_object) == false_object);
  assert((true_object & na_object).is_na());
  assert((false_object & true_object) == false_object);
  assert((false_object & false_object) == false_object);
  assert((false_object & na_object) == false_object);
  assert((na_object & true_object).is_na());
  assert((na_object & false_object) == false_object);
  assert((na_object & na_object).is_na());

  assert((true_object | true_object) == true_object);
  assert((true_object | false_object) == true_object);
  assert((true_object | na_object) == true_object);
  assert((false_object | true_object) == true_object);
  assert((false_object | false_object) == false_object);
  assert((false_object | na_object).is_na());
  assert((na_object | true_object) == true_object);
  assert((na_object | false_object).is_na());
  assert((na_object | na_object).is_na());

  assert((true_object ^ true_object) == false_object);
  assert((true_object ^ false_object) == true_object);
  assert((true_object ^ na_object).is_na());
  assert((false_object ^ true_object) == true_object);
  assert((false_object ^ false_object) == false_object);
  assert((false_object ^ na_object).is_na());
  assert((na_object ^ true_object).is_na());
  assert((na_object ^ false_object).is_na());
  assert((na_object ^ na_object).is_na());

  assert((true_object == true_object) == true_object);
  assert((true_object == false_object) == false_object);
  assert((true_object == na_object).is_na());
  assert((false_object == true_object) == false_object);
  assert((false_object == false_object) == true_object);
  assert((false_object == na_object).is_na());
  assert((na_object == true_object).is_na());
  assert((na_object == false_object).is_na());
  assert((na_object == na_object).is_na());

  assert((true_object != true_object) == false_object);
  assert((true_object != false_object) == true_object);
  assert((true_object != na_object).is_na());
  assert((false_object != true_object) == true_object);
  assert((false_object != false_object) == false_object);
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

  assert(+grnxx::Int(0) == grnxx::Int(0));
  assert(+grnxx::Int(1) == grnxx::Int(1));
  assert(+grnxx::Int::min() == grnxx::Int::min());
  assert(+grnxx::Int::max() == grnxx::Int::max());
  assert((+grnxx::Int::na()).is_na());

  assert(-grnxx::Int(0) == grnxx::Int(0));
  assert(-grnxx::Int(1) == grnxx::Int(-1));
  assert(-grnxx::Int::min() == grnxx::Int::max());
  assert(-grnxx::Int::max() == grnxx::Int::min());
  assert((-grnxx::Int::na()).is_na());

  assert(~grnxx::Int(0) == grnxx::Int(-1));
  assert(~grnxx::Int(1) == grnxx::Int(-2));
  assert(~grnxx::Int::min() == grnxx::Int(grnxx::Int::max_value() - 1));
  assert((~grnxx::Int::max()).is_na());
  assert((~grnxx::Int::na()).is_na());

  grnxx::Int object(0);

  assert(++object == grnxx::Int(1));
  assert(object++ == grnxx::Int(1));
  assert(object == grnxx::Int(2));

  assert(--object == grnxx::Int(1));
  assert(object-- == grnxx::Int(1));
  assert(object == grnxx::Int(0));

  object = grnxx::Int::na();

  assert((++object).is_na());
  assert((object++).is_na());
  assert(object.is_na());

  assert((--object).is_na());
  assert((object--).is_na());
  assert(object.is_na());

  assert((grnxx::Int(0) & grnxx::Int(0)) == grnxx::Int(0));
  assert((grnxx::Int(0) & grnxx::Int(1)) == grnxx::Int(0));
  assert((grnxx::Int(0) & grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) & grnxx::Int(0)) == grnxx::Int(0));
  assert((grnxx::Int(1) & grnxx::Int(1)) == grnxx::Int(1));
  assert((grnxx::Int(1) & grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() & grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() & grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() & grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) | grnxx::Int(0)) == grnxx::Int(0));
  assert((grnxx::Int(0) | grnxx::Int(1)) == grnxx::Int(1));
  assert((grnxx::Int(0) | grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) | grnxx::Int(0)) == grnxx::Int(1));
  assert((grnxx::Int(1) | grnxx::Int(1)) == grnxx::Int(1));
  assert((grnxx::Int(1) | grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() | grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() | grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() | grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) ^ grnxx::Int(0)) == grnxx::Int(0));
  assert((grnxx::Int(0) ^ grnxx::Int(1)) == grnxx::Int(1));
  assert((grnxx::Int(0) ^ grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) ^ grnxx::Int(0)) == grnxx::Int(1));
  assert((grnxx::Int(1) ^ grnxx::Int(1)) == grnxx::Int(0));
  assert((grnxx::Int(1) ^ grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int::na()).is_na());

  object = grnxx::Int(3);

  assert((object &= grnxx::Int(1)) == grnxx::Int(1));
  assert(object == grnxx::Int(1));
  assert((object |= grnxx::Int(2)) == grnxx::Int(3));
  assert(object == grnxx::Int(3));
  assert((object ^= grnxx::Int(6)) == grnxx::Int(5));
  assert(object == grnxx::Int(5));

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

  assert((grnxx::Int(1) << grnxx::Int(0)) == grnxx::Int(1));
  assert((grnxx::Int(1) << grnxx::Int(1)) == grnxx::Int(2));
  assert((grnxx::Int(1) << grnxx::Int(63)).is_na());
  assert((grnxx::Int(1) << grnxx::Int(64)).is_na());
  assert((grnxx::Int(1) << grnxx::Int(-1)).is_na());
  assert((grnxx::Int(1) << grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() << grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() << grnxx::Int::na()).is_na());

  assert((grnxx::Int(4) >> grnxx::Int(0)) == grnxx::Int(4));
  assert((grnxx::Int(4) >> grnxx::Int(1)) == grnxx::Int(2));
  assert((grnxx::Int(4) >> grnxx::Int(63)) == grnxx::Int(0));
  assert((grnxx::Int(4) >> grnxx::Int(64)).is_na());
  assert((grnxx::Int(4) >> grnxx::Int(-1)).is_na());
  assert((grnxx::Int(4) >> grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() >> grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() >> grnxx::Int::na()).is_na());

  object = grnxx::Int(1);

  assert((object <<= grnxx::Int(3)) == grnxx::Int(8));
  assert(object == grnxx::Int(8));
  assert((object >>= grnxx::Int(2)) == grnxx::Int(2));
  assert(object == grnxx::Int(2));

  object = grnxx::Int(-1);

  assert(object.arithmetic_right_shift(grnxx::Int(0)) == grnxx::Int(-1));
  assert(object.arithmetic_right_shift(grnxx::Int(1)) == grnxx::Int(-1));

  assert(object.logical_right_shift(grnxx::Int(0)) == grnxx::Int(-1));
  assert(object.logical_right_shift(grnxx::Int(1)).is_max());

  assert((grnxx::Int(1) + grnxx::Int(1)) == grnxx::Int(2));
  assert((grnxx::Int(1) + grnxx::Int::max()).is_na());
  assert((grnxx::Int(1) + grnxx::Int::na()).is_na());
  assert((grnxx::Int(-1) + grnxx::Int(-1)) == grnxx::Int(-2));
  assert((grnxx::Int(-1) + grnxx::Int::min()).is_na());
  assert((grnxx::Int(-1) + grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() + grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() + grnxx::Int::na()).is_na());

  assert((grnxx::Int(1) - grnxx::Int(1)) == grnxx::Int(0));
  assert((grnxx::Int(1) - grnxx::Int::min()).is_na());
  assert((grnxx::Int(1) - grnxx::Int::na()).is_na());
  assert((grnxx::Int(-1) - grnxx::Int(-1)) == grnxx::Int(0));
  assert((grnxx::Int(-1) - grnxx::Int::max()).is_na());
  assert((grnxx::Int(-1) - grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() - grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() - grnxx::Int::na()).is_na());

  assert((grnxx::Int(1) * grnxx::Int(0)) == grnxx::Int(0));
  assert((grnxx::Int(1) * grnxx::Int(2)) == grnxx::Int(2));
  assert((grnxx::Int(1) * grnxx::Int::min()).is_min());
  assert((grnxx::Int(1) * grnxx::Int::max()).is_max());
  assert((grnxx::Int(1) * grnxx::Int::na()).is_na());
  assert((grnxx::Int(2) * grnxx::Int(0)) == grnxx::Int(0));
  assert((grnxx::Int(2) * grnxx::Int(2)) == grnxx::Int(4));
  assert((grnxx::Int(2) * grnxx::Int::min()).is_na());
  assert((grnxx::Int(2) * grnxx::Int::max()).is_na());
  assert((grnxx::Int(2) * grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() * grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() * grnxx::Int(2)).is_na());
  assert((grnxx::Int::na() * grnxx::Int::na()).is_na());

  object = grnxx::Int(1);

  assert((object += grnxx::Int(2)) == grnxx::Int(3));
  assert(object == grnxx::Int(3));
  assert((object -= grnxx::Int(1)) == grnxx::Int(2));
  assert(object == grnxx::Int(2));
  assert((object *= grnxx::Int(4)) == grnxx::Int(8));
  assert(object == grnxx::Int(8));

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
  assert((grnxx::Int(0) / grnxx::Int(1)) == grnxx::Int(0));
  assert((grnxx::Int(0) / grnxx::Int(2)) == grnxx::Int(0));
  assert((grnxx::Int(0) / grnxx::Int::na()).is_na());
  assert((grnxx::Int(2) / grnxx::Int(0)).is_na());
  assert((grnxx::Int(2) / grnxx::Int(1)) == grnxx::Int(2));
  assert((grnxx::Int(2) / grnxx::Int(2)) == grnxx::Int(1));
  assert((grnxx::Int(2) / grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) / grnxx::Int(2)) == grnxx::Int(1));
  assert((grnxx::Int(3) / grnxx::Int(-2)) == grnxx::Int(-1));
  assert((grnxx::Int(-3) / grnxx::Int(2)) == grnxx::Int(-1));
  assert((grnxx::Int(-3) / grnxx::Int(-2)) == grnxx::Int(1));
  assert((grnxx::Int::min() / grnxx::Int(-1)).is_max());
  assert((grnxx::Int::max() / grnxx::Int(-1)).is_min());
  assert((grnxx::Int::na() / grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() / grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() / grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) % grnxx::Int(0)).is_na());
  assert((grnxx::Int(0) % grnxx::Int(1)) == grnxx::Int(0));
  assert((grnxx::Int(0) % grnxx::Int(2)) == grnxx::Int(0));
  assert((grnxx::Int(0) % grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) % grnxx::Int(0)).is_na());
  assert((grnxx::Int(3) % grnxx::Int(1)) == grnxx::Int(0));
  assert((grnxx::Int(3) % grnxx::Int(2)) == grnxx::Int(1));
  assert((grnxx::Int(3) % grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) % grnxx::Int(-2)) == grnxx::Int(1));
  assert((grnxx::Int(-3) % grnxx::Int(2)) == grnxx::Int(-1));
  assert((grnxx::Int(-3) % grnxx::Int(-2)) == grnxx::Int(-1));
  assert((grnxx::Int::na() % grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() % grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() % grnxx::Int::na()).is_na());

  object = grnxx::Int(13);

  assert((object /= grnxx::Int(2)) == grnxx::Int(6));
  assert(object == grnxx::Int(6));
  assert((object %= grnxx::Int(3)) == grnxx::Int(0));
  assert(object == grnxx::Int(0));

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
  // TODO
}

void test_geo_point() {
  // TODO
}

int main() {
  test_bool();
  test_int();
  test_float();
  test_geo_point();
  return 0;
}
