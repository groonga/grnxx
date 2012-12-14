#!/bin/sh

GRNXX_VERSION_HEADER=version.h

if [ -d "../.git" -o -f "../.git" ]
then
  GRNXX_CURRENT_VERSION=`git describe --abbrev=7 HEAD 2>/dev/null`
  GRNXX_CURRENT_VERSION=`expr "${GRNXX_CURRENT_VERSION}" : v*'\(.*\)'`

  if [ -r ${GRNXX_VERSION_HEADER} ]
  then
    GRNXX_OLD_VERSION=`grep '^#define GRNXX_VERSION ' ${GRNXX_VERSION_HEADER} |\
                       sed -e 's/^#define GRNXX_VERSION "\(.*\)"/\1/'`
  else
    GRNXX_OLD_VERSION=unset
  fi

  if [ "${GRNXX_OLD_VERSION}" != "${GRNXX_CURRENT_VERSION}" ]
  then
    echo "#define GRNXX_VERSION \"${GRNXX_CURRENT_VERSION}\""\
         >${GRNXX_VERSION_HEADER}
  fi
fi
