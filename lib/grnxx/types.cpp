#include "grnxx/types.hpp"

namespace grnxx {

void GeoPoint::fix(Int *latitude, Int *longitude) {
  // Fix the latitude to [0, 360).
  if ((*latitude <= DEGREES(-360)) || (*latitude >= DEGREES(360))) {
    *latitude %= DEGREES(360);
  }
  if (*latitude < DEGREES(0)) {
    *latitude += DEGREES(360);
  }
  // Fix the longitude to [0, 360).
  if ((*longitude <= DEGREES(-360)) || (*longitude >= DEGREES(360))) {
    *longitude %= DEGREES(360);
  }
  if (*longitude < DEGREES(0)) {
    *longitude += DEGREES(360);
  }
  // Fix the latitude in (90, 270).
  if ((*latitude > DEGREES(90)) && (*latitude < DEGREES(270))) {
    *latitude = DEGREES(180) - *latitude;
    if (*latitude < DEGREES(0)) {
      *latitude += DEGREES(360);
    }
    *longitude += DEGREES(180);
    if (*longitude >= DEGREES(360)) {
      *longitude -= DEGREES(360);
    }
  }
  // Fix the latitude to [-90, 90].
  if (*latitude >= DEGREES(270)) {
    *latitude -= DEGREES(360);
  }
  // Fix the longitude to [-180, 180).
  if (*longitude >= DEGREES(180)) {
    *longitude -= DEGREES(360);
  }
}

DBOptions::DBOptions() {}

TableOptions::TableOptions() {}

ColumnOptions::ColumnOptions() {}

IndexOptions::IndexOptions() {}

CursorOptions::CursorOptions()
    : offset(0),
      limit(numeric_limits<Int>::max()),
      order_type(REGULAR_ORDER) {}

}  // namespace grnxx
