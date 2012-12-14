#!/bin/sh

TOP_DIR="${TOP_SRCDIR:-..}"
LIB_DIR="${TOP_DIR}/lib"
GIT_DIR="${TOP_DIR}/.git"

GRNXX_VERSION_HEADER="version.h"

echo "GRNXX_VERSION_HEADER = ${GRNXX_VERSION_HEADER}"

if [ -d ${GIT_DIR} ]
then
  GRNXX_CURRENT_VERSION=`(cd ${TOP_DIR}; git describe --abbrev=7 HEAD 2>/dev/null)`
  GRNXX_CURRENT_VERSION=`expr "${GRNXX_CURRENT_VERSION}" : v*'\(.*\)'`

  echo "GRNXX_CURRENT_VERSION = ${GRNXX_CURRENT_VERSION}"

  if [ -r ${GRNXX_VERSION_HEADER} ]
  then
    GRNXX_OLD_VERSION=`grep '^#define GRNXX_VERSION ' ${GRNXX_VERSION_HEADER} |\
                       sed -e 's/^#define GRNXX_VERSION "\(.*\)"/\1/'`
  else
    GRNXX_OLD_VERSION=unset
  fi

  echo "GRNXX_OLD_VERSION = ${GRNXX_OLD_VERSION}"

  if [ "${GRNXX_OLD_VERSION}" != "${GRNXX_CURRENT_VERSION}" ]
  then
    echo "#define GRNXX_VERSION \"${GRNXX_CURRENT_VERSION}\""\
         >${GRNXX_VERSION_HEADER}
  fi
fi
