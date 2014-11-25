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

  assert(true_object.raw() == grnxx::Bool::raw_true());
  assert(false_object.raw() == grnxx::Bool::raw_false());
  assert(na_object.raw() == grnxx::Bool::raw_na());

  assert(true_object.is_true());
  assert(!true_object.is_false());
  assert(!true_object.is_na());

  assert(!false_object.is_true());
  assert(false_object.is_false());
  assert(!false_object.is_na());

  assert(!na_object.is_true());
  assert(!na_object.is_false());
  assert(na_object.is_na());

  assert((!true_object).is_false());
  assert((!false_object).is_true());
  assert((!na_object).is_na());

  assert((~true_object).is_false());
  assert((~false_object).is_true());
  assert((~na_object).is_na());

  assert((true_object & true_object).is_true());
  assert((true_object & false_object).is_false());
  assert((true_object & na_object).is_na());
  assert((false_object & true_object).is_false());
  assert((false_object & false_object).is_false());
  assert((false_object & na_object).is_false());
  assert((na_object & true_object).is_na());
  assert((na_object & false_object).is_false());
  assert((na_object & na_object).is_na());

  assert((true_object | true_object).is_true());
  assert((true_object | false_object).is_true());
  assert((true_object | na_object).is_true());
  assert((false_object | true_object).is_true());
  assert((false_object | false_object).is_false());
  assert((false_object | na_object).is_na());
  assert((na_object | true_object).is_true());
  assert((na_object | false_object).is_na());
  assert((na_object | na_object).is_na());

  assert((true_object ^ true_object).is_false());
  assert((true_object ^ false_object).is_true());
  assert((true_object ^ na_object).is_na());
  assert((false_object ^ true_object).is_true());
  assert((false_object ^ false_object).is_false());
  assert((false_object ^ na_object).is_na());
  assert((na_object ^ true_object).is_na());
  assert((na_object ^ false_object).is_na());
  assert((na_object ^ na_object).is_na());

  assert((true_object == true_object).is_true());
  assert((true_object == false_object).is_false());
  assert((true_object == na_object).is_na());
  assert((false_object == true_object).is_false());
  assert((false_object == false_object).is_true());
  assert((false_object == na_object).is_na());
  assert((na_object == true_object).is_na());
  assert((na_object == false_object).is_na());
  assert((na_object == na_object).is_na());

  assert((true_object != true_object).is_false());
  assert((true_object != false_object).is_true());
  assert((true_object != na_object).is_na());
  assert((false_object != true_object).is_true());
  assert((false_object != false_object).is_false());
  assert((false_object != na_object).is_na());
  assert((na_object != true_object).is_na());
  assert((na_object != false_object).is_na());
  assert((na_object != na_object).is_na());

  assert(true_object.match(true_object));
  assert(!true_object.match(false_object));
  assert(!true_object.match(na_object));
  assert(!false_object.match(true_object));
  assert(false_object.match(false_object));
  assert(!false_object.match(na_object));
  assert(!na_object.match(true_object));
  assert(!na_object.match(false_object));
  assert(na_object.match(na_object));

  assert(!true_object.unmatch(true_object));
  assert(true_object.unmatch(false_object));
  assert(true_object.unmatch(na_object));
  assert(false_object.unmatch(true_object));
  assert(!false_object.unmatch(false_object));
  assert(false_object.unmatch(na_object));
  assert(na_object.unmatch(true_object));
  assert(na_object.unmatch(false_object));
  assert(!na_object.unmatch(na_object));

  assert(grnxx::Bool::na().is_na());
}

void test_int() {
  assert(grnxx::Int(0).type() == grnxx::INT_DATA);
  assert(grnxx::Int::min().type() == grnxx::INT_DATA);
  assert(grnxx::Int::max().type() == grnxx::INT_DATA);
  assert(grnxx::Int::na().type() == grnxx::INT_DATA);

  assert(grnxx::Int(0).raw() == 0);
  assert(grnxx::Int::min().raw() == grnxx::Int::raw_min());
  assert(grnxx::Int::max().raw() == grnxx::Int::raw_max());
  assert(grnxx::Int::na().raw() == grnxx::Int::raw_na());

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

  assert((+grnxx::Int(0)).raw() == 0);
  assert((+grnxx::Int(1)).raw() == 1);
  assert((+grnxx::Int::min()).is_min());
  assert((+grnxx::Int::max()).is_max());
  assert((+grnxx::Int::na()).is_na());

  assert((-grnxx::Int(0)).raw() == 0);
  assert((-grnxx::Int(1)).raw() == -1);
  assert((-grnxx::Int::min()).is_max());
  assert((-grnxx::Int::max()).is_min());
  assert((-grnxx::Int::na()).is_na());

  assert((~grnxx::Int(0)).raw() == -1);
  assert((~grnxx::Int(1)).raw() == -2);
  assert((~grnxx::Int::min()).raw() == (grnxx::Int::raw_max() - 1));
  assert((~grnxx::Int::max()).is_na());
  assert((~grnxx::Int::na()).is_na());

  grnxx::Int object(0);

  assert((++object).raw() == 1);
  assert((object++).raw() == 1);
  assert(object.raw() == 2);

  assert((--object).raw() == 1);
  assert((object--).raw() == 1);
  assert(object.raw() == 0);

  object = grnxx::Int::na();

  assert((++object).is_na());
  assert((object++).is_na());
  assert(object.is_na());

  assert((--object).is_na());
  assert((object--).is_na());
  assert(object.is_na());

  assert((grnxx::Int(0) & grnxx::Int(0)).raw() == 0);
  assert((grnxx::Int(0) & grnxx::Int(1)).raw() == 0);
  assert((grnxx::Int(0) & grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) & grnxx::Int(0)).raw() == 0);
  assert((grnxx::Int(1) & grnxx::Int(1)).raw() == 1);
  assert((grnxx::Int(1) & grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() & grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() & grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() & grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) | grnxx::Int(0)).raw() == 0);
  assert((grnxx::Int(0) | grnxx::Int(1)).raw() == 1);
  assert((grnxx::Int(0) | grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) | grnxx::Int(0)).raw() == 1);
  assert((grnxx::Int(1) | grnxx::Int(1)).raw() == 1);
  assert((grnxx::Int(1) | grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() | grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() | grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() | grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) ^ grnxx::Int(0)).raw() == 0);
  assert((grnxx::Int(0) ^ grnxx::Int(1)).raw() == 1);
  assert((grnxx::Int(0) ^ grnxx::Int::na()).is_na());
  assert((grnxx::Int(1) ^ grnxx::Int(0)).raw() == 1);
  assert((grnxx::Int(1) ^ grnxx::Int(1)).raw() == 0);
  assert((grnxx::Int(1) ^ grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() ^ grnxx::Int::na()).is_na());

  object = grnxx::Int(3);

  assert((object &= grnxx::Int(1)).raw() == 1);
  assert(object.raw() == 1);
  assert((object |= grnxx::Int(2)).raw() == 3);
  assert(object.raw() == 3);
  assert((object ^= grnxx::Int(6)).raw() == 5);
  assert(object.raw() == 5);

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

  assert((grnxx::Int(1) << grnxx::Int(0)).raw() == 1);
  assert((grnxx::Int(1) << grnxx::Int(1)).raw() == 2);
  assert((grnxx::Int(1) << grnxx::Int(63)).is_na());
  assert((grnxx::Int(1) << grnxx::Int(64)).is_na());
  assert((grnxx::Int(1) << grnxx::Int(-1)).is_na());
  assert((grnxx::Int(1) << grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() << grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() << grnxx::Int::na()).is_na());

  assert((grnxx::Int(4) >> grnxx::Int(0)).raw() == 4);
  assert((grnxx::Int(4) >> grnxx::Int(1)).raw() == 2);
  assert((grnxx::Int(4) >> grnxx::Int(63)).raw() == 0);
  assert((grnxx::Int(4) >> grnxx::Int(64)).is_na());
  assert((grnxx::Int(4) >> grnxx::Int(-1)).is_na());
  assert((grnxx::Int(4) >> grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() >> grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() >> grnxx::Int::na()).is_na());

  object = grnxx::Int(1);

  assert((object <<= grnxx::Int(3)).raw() == 8);
  assert(object.raw() == 8);
  assert((object >>= grnxx::Int(2)).raw() == 2);
  assert(object.raw() == 2);

  object = grnxx::Int(-1);

  assert(object.arithmetic_right_shift(grnxx::Int(0)).raw() == -1);
  assert(object.arithmetic_right_shift(grnxx::Int(1)).raw() == -1);

  assert(object.logical_right_shift(grnxx::Int(0)).raw() == -1);
  assert(object.logical_right_shift(grnxx::Int(1)).is_max());

  assert((grnxx::Int(1) + grnxx::Int(1)).raw() == 2);
  assert((grnxx::Int(1) + grnxx::Int::max()).is_na());
  assert((grnxx::Int(1) + grnxx::Int::na()).is_na());
  assert((grnxx::Int(-1) + grnxx::Int(-1)).raw() == -2);
  assert((grnxx::Int(-1) + grnxx::Int::min()).is_na());
  assert((grnxx::Int(-1) + grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() + grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() + grnxx::Int::na()).is_na());

  assert((grnxx::Int(1) - grnxx::Int(1)).raw() == 0);
  assert((grnxx::Int(1) - grnxx::Int::min()).is_na());
  assert((grnxx::Int(1) - grnxx::Int::na()).is_na());
  assert((grnxx::Int(-1) - grnxx::Int(-1)).raw() == 0);
  assert((grnxx::Int(-1) - grnxx::Int::max()).is_na());
  assert((grnxx::Int(-1) - grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() - grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() - grnxx::Int::na()).is_na());

  assert((grnxx::Int(1) * grnxx::Int(0)).raw() == 0);
  assert((grnxx::Int(1) * grnxx::Int(2)).raw() == 2);
  assert((grnxx::Int(1) * grnxx::Int::min()).is_min());
  assert((grnxx::Int(1) * grnxx::Int::max()).is_max());
  assert((grnxx::Int(1) * grnxx::Int::na()).is_na());
  assert((grnxx::Int(2) * grnxx::Int(0)).raw() == 0);
  assert((grnxx::Int(2) * grnxx::Int(2)).raw() == 4);
  assert((grnxx::Int(2) * grnxx::Int::min()).is_na());
  assert((grnxx::Int(2) * grnxx::Int::max()).is_na());
  assert((grnxx::Int(2) * grnxx::Int::na()).is_na());
  assert((grnxx::Int::na() * grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() * grnxx::Int(2)).is_na());
  assert((grnxx::Int::na() * grnxx::Int::na()).is_na());

  object = grnxx::Int(1);

  assert((object += grnxx::Int(2)).raw() == 3);
  assert(object.raw() == 3);
  assert((object -= grnxx::Int(1)).raw() == 2);
  assert(object.raw() == 2);
  assert((object *= grnxx::Int(4)).raw() == 8);
  assert(object.raw() == 8);

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
  assert((grnxx::Int(0) / grnxx::Int(1)).raw() == 0);
  assert((grnxx::Int(0) / grnxx::Int(2)).raw() == 0);
  assert((grnxx::Int(0) / grnxx::Int::na()).is_na());
  assert((grnxx::Int(2) / grnxx::Int(0)).is_na());
  assert((grnxx::Int(2) / grnxx::Int(1)).raw() == 2);
  assert((grnxx::Int(2) / grnxx::Int(2)).raw() == 1);
  assert((grnxx::Int(2) / grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) / grnxx::Int(2)).raw() == 1);
  assert((grnxx::Int(3) / grnxx::Int(-2)).raw() == -1);
  assert((grnxx::Int(-3) / grnxx::Int(2)).raw() == -1);
  assert((grnxx::Int(-3) / grnxx::Int(-2)).raw() == 1);
  assert((grnxx::Int::min() / grnxx::Int(-1)).is_max());
  assert((grnxx::Int::max() / grnxx::Int(-1)).is_min());
  assert((grnxx::Int::na() / grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() / grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() / grnxx::Int::na()).is_na());

  assert((grnxx::Int(0) % grnxx::Int(0)).is_na());
  assert((grnxx::Int(0) % grnxx::Int(1)).raw() == 0);
  assert((grnxx::Int(0) % grnxx::Int(2)).raw() == 0);
  assert((grnxx::Int(0) % grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) % grnxx::Int(0)).is_na());
  assert((grnxx::Int(3) % grnxx::Int(1)).raw() == 0);
  assert((grnxx::Int(3) % grnxx::Int(2)).raw() == 1);
  assert((grnxx::Int(3) % grnxx::Int::na()).is_na());
  assert((grnxx::Int(3) % grnxx::Int(-2)).raw() == 1);
  assert((grnxx::Int(-3) % grnxx::Int(2)).raw() == -1);
  assert((grnxx::Int(-3) % grnxx::Int(-2)).raw() == -1);
  assert((grnxx::Int::na() % grnxx::Int(0)).is_na());
  assert((grnxx::Int::na() % grnxx::Int(1)).is_na());
  assert((grnxx::Int::na() % grnxx::Int::na()).is_na());

  object = grnxx::Int(13);

  assert((object /= grnxx::Int(2)).raw() == 6);
  assert(object.raw() == 6);
  assert((object %= grnxx::Int(3)).raw() == 0);
  assert(object.raw() == 0);

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

  assert(grnxx::Int(0).match(grnxx::Int(0)));
  assert(!grnxx::Int(0).match(grnxx::Int(1)));
  assert(!grnxx::Int(0).match(grnxx::Int::na()));
  assert(!grnxx::Int(1).match(grnxx::Int(0)));
  assert(grnxx::Int(1).match(grnxx::Int(1)));
  assert(!grnxx::Int(1).match(grnxx::Int::na()));
  assert(!grnxx::Int::na().match(grnxx::Int(0)));
  assert(!grnxx::Int::na().match(grnxx::Int(1)));
  assert(grnxx::Int::na().match(grnxx::Int::na()));

  assert(!grnxx::Int(0).unmatch(grnxx::Int(0)));
  assert(grnxx::Int(0).unmatch(grnxx::Int(1)));
  assert(grnxx::Int(0).unmatch(grnxx::Int::na()));
  assert(grnxx::Int(1).unmatch(grnxx::Int(0)));
  assert(!grnxx::Int(1).unmatch(grnxx::Int(1)));
  assert(grnxx::Int(1).unmatch(grnxx::Int::na()));
  assert(grnxx::Int::na().unmatch(grnxx::Int(0)));
  assert(grnxx::Int::na().unmatch(grnxx::Int(1)));
  assert(!grnxx::Int::na().unmatch(grnxx::Int::na()));
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

  assert(grnxx::Float(0.0).raw() == 0.0);
  assert(grnxx::Float::min().is_min());
  assert(grnxx::Float::max().is_max());
  assert(grnxx::Float::normal_min().raw() ==
         grnxx::Float::raw_normal_min());
  assert(grnxx::Float::subnormal_min().raw() ==
         grnxx::Float::raw_subnormal_min());
  assert(grnxx::Float::infinity().raw() ==
         grnxx::Float::raw_infinity());
  assert(std::isnan(grnxx::Float::na().raw()));

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

  assert((+grnxx::Float(0.0)).raw() == 0.0);
  assert((+grnxx::Float(1.0)).raw() == 1.0);
  assert((+grnxx::Float::min()).is_min());
  assert((+grnxx::Float::max()).is_max());
  assert((+grnxx::Float::infinity()).raw() ==
         grnxx::Float::raw_infinity());
  assert((+grnxx::Float::na()).is_na());

  assert((-grnxx::Float(0.0)).raw() == 0.0);
  assert((-grnxx::Float(1.0)).raw() == -1.0);
  assert((-grnxx::Float::min()).is_max());
  assert((-grnxx::Float::max()).is_min());
  assert((-grnxx::Float::infinity()).raw() ==
         -grnxx::Float::raw_infinity());
  assert((-grnxx::Float::na()).is_na());

  assert((grnxx::Float(1.0) + grnxx::Float(1.0)).raw() == 2.0);
  assert((grnxx::Float::max() + grnxx::Float::max()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() + grnxx::Float::min()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() + -grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(1.0) + grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() + grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() + grnxx::Float::na()).is_na());

  assert((grnxx::Float(1.0) - grnxx::Float(1.0)).raw() == 0.0);
  assert((grnxx::Float::max() - -grnxx::Float::max()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() - grnxx::Float::max()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() - grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(1.0) - grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() - grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() - grnxx::Float::na()).is_na());

  assert((grnxx::Float(2.0) * grnxx::Float(0.5)).raw() == 1.0);
  assert((grnxx::Float::max() * grnxx::Float::max()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() * grnxx::Float::subnormal_min()).raw()
         == grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() * grnxx::Float(0.0)).is_na());
  assert((grnxx::Float(1.0) * grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() * grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() * grnxx::Float::na()).is_na());

  grnxx::Float object(1.0);

  assert((object += grnxx::Float(2.0)).raw() == 3.0);
  assert(object.raw() == 3.0);
  assert((object -= grnxx::Float(1.0)).raw() == 2.0);
  assert(object.raw() == 2.0);
  assert((object *= grnxx::Float(4.0)).raw() == 8.0);
  assert(object.raw() == 8.0);

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

  assert((grnxx::Float(1.0) / grnxx::Float(2.0)).raw() == 0.5);
  assert((grnxx::Float(1.0) / grnxx::Float(0.0)).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float(1.0) / grnxx::Float::infinity()).raw() == 0.0);
  assert((grnxx::Float::max() / grnxx::Float::subnormal_min()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() / grnxx::Float::max()).raw() ==
         grnxx::Float::raw_infinity());
  assert((grnxx::Float::infinity() / grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(0.0) / grnxx::Float(0.0)).is_na());
  assert((grnxx::Float(1.0) / grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() / grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() / grnxx::Float::na()).is_na());

  assert((grnxx::Float(1.0) % grnxx::Float(2.0)).raw() == 1.0);
  assert((grnxx::Float(1.0) % grnxx::Float(-2.0)).raw() == 1.0);
  assert((grnxx::Float(-1.0) % grnxx::Float(2.0)).raw() == -1.0);
  assert((grnxx::Float(-1.0) % grnxx::Float(-2.0)).raw() == -1.0);
  assert((grnxx::Float(1.0) % grnxx::Float::infinity()).raw() == 1.0);
  assert((grnxx::Float::infinity() % grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::infinity() % grnxx::Float::infinity()).is_na());
  assert((grnxx::Float(0.0) % grnxx::Float(0.0)).is_na());
  assert((grnxx::Float(1.0) % grnxx::Float::na()).is_na());
  assert((grnxx::Float::na() % grnxx::Float(1.0)).is_na());
  assert((grnxx::Float::na() % grnxx::Float::na()).is_na());

  object = grnxx::Float(13.0);

  assert((object /= grnxx::Float(2.0)).raw() == 6.5);
  assert(object.raw() == 6.5);
  assert((object %= grnxx::Float(3.0)).raw() == 0.5);
  assert(object.raw() == 0.5);

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

  assert(grnxx::Float::min().match(grnxx::Float::min()));
  assert(!grnxx::Float::min().match(grnxx::Float::max()));
  assert(!grnxx::Float::min().match(grnxx::Float::infinity()));
  assert(!grnxx::Float::min().match(grnxx::Float::na()));
  assert(!grnxx::Float::max().match(grnxx::Float::min()));
  assert(grnxx::Float::max().match(grnxx::Float::max()));
  assert(!grnxx::Float::max().match(grnxx::Float::infinity()));
  assert(!grnxx::Float::max().match(grnxx::Float::na()));
  assert(!grnxx::Float::infinity().match(grnxx::Float::min()));
  assert(!grnxx::Float::infinity().match(grnxx::Float::max()));
  assert(grnxx::Float::infinity().match(grnxx::Float::infinity()));
  assert(!grnxx::Float::infinity().match(grnxx::Float::na()));
  assert(!grnxx::Float::na().match(grnxx::Float::min()));
  assert(!grnxx::Float::na().match(grnxx::Float::max()));
  assert(!grnxx::Float::na().match(grnxx::Float::infinity()));
  assert(grnxx::Float::na().match(grnxx::Float::na()));

  assert(!grnxx::Float::min().unmatch(grnxx::Float::min()));
  assert(grnxx::Float::min().unmatch(grnxx::Float::max()));
  assert(grnxx::Float::min().unmatch(grnxx::Float::infinity()));
  assert(grnxx::Float::min().unmatch(grnxx::Float::na()));
  assert(grnxx::Float::max().unmatch(grnxx::Float::min()));
  assert(!grnxx::Float::max().unmatch(grnxx::Float::max()));
  assert(grnxx::Float::max().unmatch(grnxx::Float::infinity()));
  assert(grnxx::Float::max().unmatch(grnxx::Float::na()));
  assert(grnxx::Float::infinity().unmatch(grnxx::Float::min()));
  assert(grnxx::Float::infinity().unmatch(grnxx::Float::max()));
  assert(!grnxx::Float::infinity().unmatch(grnxx::Float::infinity()));
  assert(grnxx::Float::infinity().unmatch(grnxx::Float::na()));
  assert(grnxx::Float::na().unmatch(grnxx::Float::min()));
  assert(grnxx::Float::na().unmatch(grnxx::Float::max()));
  assert(grnxx::Float::na().unmatch(grnxx::Float::infinity()));
  assert(!grnxx::Float::na().unmatch(grnxx::Float::na()));

  assert((grnxx::Float(0.0).next_toward(grnxx::Float::max())).raw() ==
         grnxx::Float::raw_subnormal_min());
  assert((grnxx::Float(0.0).next_toward(-grnxx::Float::max())).raw() ==
         -grnxx::Float::raw_subnormal_min());
  assert((grnxx::Float(0.0).next_toward(grnxx::Float::infinity())).raw() ==
         grnxx::Float::raw_subnormal_min());
  assert((grnxx::Float(0.0).next_toward(-grnxx::Float::infinity())).raw() ==
         -grnxx::Float::raw_subnormal_min());
  assert((grnxx::Float::infinity().next_toward(grnxx::Float(0.0))).raw()
         == grnxx::Float::raw_max());
  assert((-grnxx::Float::infinity().next_toward(grnxx::Float(0.0))).raw()
         == grnxx::Float::raw_min());
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

  assert(zero.raw_latitude() == 0);
  assert(date_line.raw_latitude() == 0);
  assert(na.raw_latitude() == grnxx::GeoPoint::raw_na_latitude());

  assert(zero.raw_longitude() == 0);
  assert(north_pole.raw_longitude() == 0);
  assert(south_pole.raw_longitude() == 0);
  assert(na.raw_longitude() == grnxx::GeoPoint::raw_na_longitude());

  assert(zero.latitude_in_milliseconds().raw() == 0);
  assert(date_line.latitude_in_milliseconds().raw() == 0);
  assert(na.latitude_in_milliseconds().is_na());

  assert(zero.longitude_in_milliseconds().raw() == 0);
  assert(north_pole.longitude_in_milliseconds().raw() == 0);
  assert(south_pole.longitude_in_milliseconds().raw() == 0);
  assert(na.longitude_in_milliseconds().is_na());

  assert(zero.latitude_in_degrees().raw() == 0.0);
  assert(north_pole.latitude_in_degrees().raw() == 90.0);
  assert(south_pole.latitude_in_degrees().raw() == -90.0);
  assert(date_line.latitude_in_degrees().raw() == 0.0);
  assert(na.latitude_in_degrees().is_na());

  assert(zero.longitude_in_degrees().raw() == 0.0);
  assert(north_pole.longitude_in_degrees().raw() == 0.0);
  assert(south_pole.longitude_in_degrees().raw() == 0.0);
  assert(date_line.longitude_in_degrees().raw() == -180.0);
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

  assert(zero.match(zero));
  assert(!zero.match(north_pole));
  assert(!zero.match(south_pole));
  assert(!zero.match(date_line));
  assert(!zero.match(na));
  assert(north_pole.match(north_pole));
  assert(!north_pole.match(south_pole));
  assert(!north_pole.match(date_line));
  assert(!north_pole.match(na));
  assert(south_pole.match(south_pole));
  assert(!south_pole.match(date_line));
  assert(!south_pole.match(na));
  assert(date_line.match(date_line));
  assert(!date_line.match(na));
  assert(na.match(na));

  assert(!zero.unmatch(zero));
  assert(zero.unmatch(north_pole));
  assert(zero.unmatch(south_pole));
  assert(zero.unmatch(date_line));
  assert(zero.unmatch(na));
  assert(!north_pole.unmatch(north_pole));
  assert(north_pole.unmatch(south_pole));
  assert(north_pole.unmatch(date_line));
  assert(north_pole.unmatch(na));
  assert(!south_pole.unmatch(south_pole));
  assert(south_pole.unmatch(date_line));
  assert(south_pole.unmatch(na));
  assert(!date_line.unmatch(date_line));
  assert(date_line.unmatch(na));
  assert(!na.unmatch(na));
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

  assert(std::strcmp(ab.raw_data(), "ab") == 0);
  assert(std::strcmp(abc.raw_data(), "abc") == 0);
  assert(std::strcmp(bc.raw_data(), "bc") == 0);

  assert(ab.size().raw() == 2);
  assert(abc.size().raw() == 3);
  assert(bc.size().raw() == 2);
  assert(empty.size().raw() == 0);
  assert(na.size().is_na());

  assert(ab.raw_size() == 2);
  assert(abc.raw_size() == 3);
  assert(bc.raw_size() == 2);
  assert(empty.raw_size() == 0);
  assert(na.raw_size() == grnxx::Text::raw_na_size());

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

  assert((ab.contains(ab)).is_true());
  assert((ab.contains(abc)).is_false());
  assert((ab.contains(bc)).is_false());
  assert((ab.contains(empty)).is_true());
  assert((ab.contains(na)).is_na());
  assert((abc.contains(ab)).is_true());
  assert((abc.contains(abc)).is_true());
  assert((abc.contains(bc)).is_true());
  assert((abc.contains(empty)).is_true());
  assert((abc.contains(na)).is_na());
  assert((bc.contains(ab)).is_false());
  assert((bc.contains(abc)).is_false());
  assert((bc.contains(bc)).is_true());
  assert((bc.contains(empty)).is_true());
  assert((bc.contains(na)).is_na());
  assert((empty.contains(ab)).is_false());
  assert((empty.contains(abc)).is_false());
  assert((empty.contains(bc)).is_false());
  assert((empty.contains(empty)).is_true());
  assert((empty.contains(na)).is_na());
  assert((na.contains(ab)).is_na());
  assert((na.contains(abc)).is_na());
  assert((na.contains(bc)).is_na());
  assert((na.contains(empty)).is_na());
  assert((na.contains(na)).is_na());

  assert(ab.match(ab));
  assert(!ab.match(abc));
  assert(!ab.match(bc));
  assert(!ab.match(empty));
  assert(!ab.match(na));
  assert(abc.match(abc));
  assert(!abc.match(bc));
  assert(!abc.match(empty));
  assert(!abc.match(na));
  assert(bc.match(bc));
  assert(!bc.match(empty));
  assert(!bc.match(na));
  assert(empty.match(empty));
  assert(!empty.match(na));
  assert(na.match(na));

  assert(!ab.unmatch(ab));
  assert(ab.unmatch(abc));
  assert(ab.unmatch(bc));
  assert(ab.unmatch(empty));
  assert(ab.unmatch(na));
  assert(!abc.unmatch(abc));
  assert(abc.unmatch(bc));
  assert(abc.unmatch(empty));
  assert(abc.unmatch(na));
  assert(!bc.unmatch(bc));
  assert(bc.unmatch(empty));
  assert(bc.unmatch(na));
  assert(!empty.unmatch(empty));
  assert(empty.unmatch(na));
  assert(!na.unmatch(na));
}

void test_bool_vector() {
  grnxx::Bool data[] = {
    grnxx::Bool(true),
    grnxx::Bool(false),
    grnxx::Bool(true),
    grnxx::Bool::na()
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

  assert(tft.size().raw() == 3);
  assert(ftn.size().raw() == 3);
  assert(empty.size().raw() == 0);
  assert(na.size().is_na());

  assert(tft.raw_size() == 3);
  assert(ftn.raw_size() == 3);
  assert(empty.raw_size() == 0);
  assert(na.raw_size() == grnxx::BoolVector::raw_na_size());

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

  assert(tft.match(tft));
  assert(!tft.match(ftn));
  assert(!tft.match(empty));
  assert(!tft.match(na));
  assert(ftn.match(ftn));
  assert(!ftn.match(empty));
  assert(!ftn.match(na));
  assert(empty.match(empty));
  assert(!empty.match(na));
  assert(na.match(na));

  assert(!tft.unmatch(tft));
  assert(tft.unmatch(ftn));
  assert(tft.unmatch(empty));
  assert(tft.unmatch(na));
  assert(!ftn.unmatch(ftn));
  assert(ftn.unmatch(empty));
  assert(ftn.unmatch(na));
  assert(!empty.unmatch(empty));
  assert(empty.unmatch(na));
  assert(!na.unmatch(na));
}

void test_int_vector() {
  grnxx::Int data[] = {
    grnxx::Int(1),
    grnxx::Int(2),
    grnxx::Int(3),
    grnxx::Int::na()
  };

  grnxx::IntVector abc = grnxx::IntVector(data, 3);
  grnxx::IntVector bcn = grnxx::IntVector(data + 1, 3);
  grnxx::IntVector empty = grnxx::IntVector::empty();
  grnxx::IntVector na = grnxx::IntVector::na();

  assert(abc.type() == grnxx::INT_VECTOR_DATA);
  assert(bcn.type() == grnxx::INT_VECTOR_DATA);
  assert(empty.type() == grnxx::INT_VECTOR_DATA);
  assert(na.type() == grnxx::INT_VECTOR_DATA);

  assert(abc[0].raw() == 1);
  assert(abc[1].raw() == 2);
  assert(abc[2].raw() == 3);

  assert(bcn[0].raw() == 2);
  assert(bcn[1].raw() == 3);
  assert(bcn[2].is_na());

  assert(abc.size().raw() == 3);
  assert(bcn.size().raw() == 3);
  assert(empty.size().raw() == 0);
  assert(na.size().is_na());

  assert(abc.raw_size() == 3);
  assert(bcn.raw_size() == 3);
  assert(empty.raw_size() == 0);
  assert(na.raw_size() == grnxx::IntVector::raw_na_size());

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

  assert(abc.match(abc));
  assert(!abc.match(bcn));
  assert(!abc.match(empty));
  assert(!abc.match(na));
  assert(bcn.match(bcn));
  assert(!bcn.match(empty));
  assert(!bcn.match(na));
  assert(empty.match(empty));
  assert(!empty.match(na));
  assert(na.match(na));

  assert(!abc.unmatch(abc));
  assert(abc.unmatch(bcn));
  assert(abc.unmatch(empty));
  assert(abc.unmatch(na));
  assert(!bcn.unmatch(bcn));
  assert(bcn.unmatch(empty));
  assert(bcn.unmatch(na));
  assert(!empty.unmatch(empty));
  assert(empty.unmatch(na));
  assert(!na.unmatch(na));
}

void test_float_vector() {
  grnxx::Float data[] = {
    grnxx::Float(1.25),
    grnxx::Float(2.50),
    grnxx::Float(6.25),
    grnxx::Float::na()
  };

  grnxx::FloatVector abc = grnxx::FloatVector(data, 3);
  grnxx::FloatVector bcn = grnxx::FloatVector(data + 1, 3);
  grnxx::FloatVector empty = grnxx::FloatVector::empty();
  grnxx::FloatVector na = grnxx::FloatVector::na();

  assert(abc.type() == grnxx::FLOAT_VECTOR_DATA);
  assert(bcn.type() == grnxx::FLOAT_VECTOR_DATA);
  assert(empty.type() == grnxx::FLOAT_VECTOR_DATA);
  assert(na.type() == grnxx::FLOAT_VECTOR_DATA);

  assert(abc[0].raw() == 1.25);
  assert(abc[1].raw() == 2.50);
  assert(abc[2].raw() == 6.25);

  assert(bcn[0].raw() == 2.50);
  assert(bcn[1].raw() == 6.25);
  assert(bcn[2].is_na());

  assert(abc.size().raw() == 3);
  assert(bcn.size().raw() == 3);
  assert(empty.size().raw() == 0);
  assert(na.size().is_na());

  assert(abc.raw_size() == 3);
  assert(bcn.raw_size() == 3);
  assert(empty.raw_size() == 0);
  assert(na.raw_size() == grnxx::FloatVector::raw_na_size());

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

  assert(abc.match(abc));
  assert(!abc.match(bcn));
  assert(!abc.match(empty));
  assert(!abc.match(na));
  assert(bcn.match(bcn));
  assert(!bcn.match(empty));
  assert(!bcn.match(na));
  assert(empty.match(empty));
  assert(!empty.match(na));
  assert(na.match(na));

  assert(!abc.unmatch(abc));
  assert(abc.unmatch(bcn));
  assert(abc.unmatch(empty));
  assert(abc.unmatch(na));
  assert(!bcn.unmatch(bcn));
  assert(bcn.unmatch(empty));
  assert(bcn.unmatch(na));
  assert(!empty.unmatch(empty));
  assert(empty.unmatch(na));
  assert(!na.unmatch(na));
}

void test_geo_point_vector() {
  grnxx::GeoPoint data[] = {
    { grnxx::Float(43.068661), grnxx::Float(141.350755) },  // Sapporo.
    { grnxx::Float(35.681382), grnxx::Float(139.766084) },  // Tokyo.
    { grnxx::Float(34.702485), grnxx::Float(135.495951) },  // Osaka.
    grnxx::GeoPoint::na()
  };

  grnxx::GeoPointVector sto = grnxx::GeoPointVector(data, 3);
  grnxx::GeoPointVector ton = grnxx::GeoPointVector(data + 1, 3);
  grnxx::GeoPointVector empty = grnxx::GeoPointVector::empty();
  grnxx::GeoPointVector na = grnxx::GeoPointVector::na();

  assert(sto.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(ton.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(empty.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(na.type() == grnxx::GEO_POINT_VECTOR_DATA);

  assert(sto[0].raw_latitude() == data[0].raw_latitude());
  assert(sto[1].raw_latitude() == data[1].raw_latitude());
  assert(sto[2].raw_latitude() == data[2].raw_latitude());

  assert(ton[0].raw_latitude() == data[1].raw_latitude());
  assert(ton[1].raw_latitude() == data[2].raw_latitude());
  assert(ton[2].raw_latitude() == data[3].raw_latitude());

  assert(sto[0].raw_longitude() == data[0].raw_longitude());
  assert(sto[1].raw_longitude() == data[1].raw_longitude());
  assert(sto[2].raw_longitude() == data[2].raw_longitude());

  assert(ton[0].raw_longitude() == data[1].raw_longitude());
  assert(ton[1].raw_longitude() == data[2].raw_longitude());
  assert(ton[2].raw_longitude() == data[3].raw_longitude());

  assert(sto.size().raw() == 3);
  assert(ton.size().raw() == 3);
  assert(empty.size().raw() == 0);
  assert(na.size().is_na());

  assert(sto.raw_size() == 3);
  assert(ton.raw_size() == 3);
  assert(empty.raw_size() == 0);
  assert(na.raw_size() == grnxx::GeoPointVector::raw_na_size());

  assert(!sto.is_empty());
  assert(!sto.is_empty());
  assert(empty.is_empty());
  assert(!na.is_empty());

  assert(!sto.is_na());
  assert(!ton.is_na());
  assert(!empty.is_na());
  assert(na.is_na());

  assert((sto == sto).is_true());
  assert((sto == ton).is_false());
  assert((sto == empty).is_false());
  assert((sto == na).is_na());
  assert((ton == ton).is_true());
  assert((ton == empty).is_false());
  assert((ton == na).is_na());
  assert((empty == empty).is_true());
  assert((empty == na).is_na());
  assert((na == na).is_na());

  assert((sto != sto).is_false());
  assert((sto != ton).is_true());
  assert((sto != empty).is_true());
  assert((sto != na).is_na());
  assert((ton != ton).is_false());
  assert((ton != empty).is_true());
  assert((ton != na).is_na());
  assert((empty != empty).is_false());
  assert((empty != na).is_na());
  assert((na != na).is_na());

  assert(sto.match(sto));
  assert(!sto.match(ton));
  assert(!sto.match(empty));
  assert(!sto.match(na));
  assert(ton.match(ton));
  assert(!ton.match(empty));
  assert(!ton.match(na));
  assert(empty.match(empty));
  assert(!empty.match(na));
  assert(na.match(na));

  assert(!sto.unmatch(sto));
  assert(sto.unmatch(ton));
  assert(sto.unmatch(empty));
  assert(sto.unmatch(na));
  assert(!ton.unmatch(ton));
  assert(ton.unmatch(empty));
  assert(ton.unmatch(na));
  assert(!empty.unmatch(empty));
  assert(empty.unmatch(na));
  assert(!na.unmatch(na));
}

void test_text_vector() {
  grnxx::Text data[] = {
    grnxx::Text("ABC"),
    grnxx::Text("BCD"),
    grnxx::Text("CDE"),
    grnxx::Text::na()
  };

  grnxx::TextVector abc = grnxx::TextVector(data, 3);
  grnxx::TextVector bcn = grnxx::TextVector(data + 1, 3);
  grnxx::TextVector empty = grnxx::TextVector::empty();
  grnxx::TextVector na = grnxx::TextVector::na();

  assert(abc.type() == grnxx::TEXT_VECTOR_DATA);
  assert(bcn.type() == grnxx::TEXT_VECTOR_DATA);
  assert(empty.type() == grnxx::TEXT_VECTOR_DATA);
  assert(na.type() == grnxx::TEXT_VECTOR_DATA);

  assert(abc[0].match(data[0]));
  assert(abc[1].match(data[1]));
  assert(abc[2].match(data[2]));

  assert(bcn[0].match(data[1]));
  assert(bcn[1].match(data[2]));
  assert(bcn[2].is_na());

  assert(abc.size().raw() == 3);
  assert(bcn.size().raw() == 3);
  assert(empty.size().raw() == 0);
  assert(na.size().is_na());

  assert(abc.raw_size() == 3);
  assert(bcn.raw_size() == 3);
  assert(empty.raw_size() == 0);
  assert(na.raw_size() == grnxx::TextVector::raw_na_size());

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

  assert(abc.match(abc));
  assert(!abc.match(bcn));
  assert(!abc.match(empty));
  assert(!abc.match(na));
  assert(bcn.match(bcn));
  assert(!bcn.match(empty));
  assert(!bcn.match(na));
  assert(empty.match(empty));
  assert(!empty.match(na));
  assert(na.match(na));

  assert(!abc.unmatch(abc));
  assert(abc.unmatch(bcn));
  assert(abc.unmatch(empty));
  assert(abc.unmatch(na));
  assert(!bcn.unmatch(bcn));
  assert(bcn.unmatch(empty));
  assert(bcn.unmatch(na));
  assert(!empty.unmatch(empty));
  assert(empty.unmatch(na));
  assert(!na.unmatch(na));
}

int main() {
  test_bool();
  test_int();
  test_float();
  test_geo_point();
  test_text();
  test_bool_vector();
  test_int_vector();
  test_float_vector();
  test_geo_point_vector();
  test_text_vector();
  return 0;
}
