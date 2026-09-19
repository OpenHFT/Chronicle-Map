#!/usr/bin/env bash
#
# Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
#

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <version-label> <allow-segment-tiering> <actual-segments|auto> <rounds> <output.csv>" >&2
  exit 2
fi

if [[ ! -f pom.xml ]]; then
  echo "run this script from the Chronicle Map checkout being measured" >&2
  exit 2
fi

research_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
harness_source=$(cd "${research_dir}/../.." && pwd)/src/test/java/net/openhft/chronicle/map/Issue400ChurnHarness.java
classpath_file=target/issue400-classpath.txt
harness_classes=target/issue400-harness-classes

java_bin=${JAVA_HOME:+${JAVA_HOME}/bin/}java
javac_bin=${JAVA_HOME:+${JAVA_HOME}/bin/}javac

mvn -q -DskipTests test-compile \
  -DincludeScope=test \
  -Dmdep.outputFile="${classpath_file}" \
  dependency:build-classpath

mkdir -p "${harness_classes}"
"${javac_bin}" -source 8 -target 8 -proc:none \
  -cp "target/test-classes:target/classes:$(<"${classpath_file}")" \
  -d "${harness_classes}" "${harness_source}"

jvm_args=()
java_version=$("${java_bin}" -version 2>&1)
if [[ "${java_version}" != *'"1.'* ]]; then
  jvm_args=(
    --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED
    --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
    --add-exports=java.base/jdk.internal.util=ALL-UNNAMED
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
    --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
    --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
    --add-opens=java.base/java.io=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
    --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED
  )
fi

"${java_bin}" "${jvm_args[@]}" \
  -cp "${harness_classes}:target/test-classes:target/classes:$(<"${classpath_file}")" \
  net.openhft.chronicle.map.Issue400ChurnHarness "$@"
