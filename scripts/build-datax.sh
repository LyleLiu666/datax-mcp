#!/usr/bin/env bash
# Build only the upstream modules exercised by this wrapper. No transfer code is patched.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
revision=80ec23d5c5328eb90ca364d2749e92dfaf44541e
source_dir="$root/.cache/upstream-datax"
runtime_dir="$root/.cache/datax"
modules=(common transformer core plugin-rdbms-util mysqlreader postgresqlwriter streamreader streamwriter)
command -v git >/dev/null
command -v mvn >/dev/null
command -v perl >/dev/null
mkdir -p "$root/.cache"
if [[ ! -d "$source_dir/.git" ]]; then
  git clone --filter=blob:none --no-checkout https://github.com/alibaba/DataX.git "$source_dir"
  git -C "$source_dir" sparse-checkout init --cone
  git -C "$source_dir" sparse-checkout set "${modules[@]}" package datax-example
  git -C "$source_dir" checkout --detach "$revision"
fi
[[ $(git -C "$source_dir" rev-parse HEAD) == "$revision" ]] || { echo 'Unexpected upstream revision' >&2; exit 1; }
# Restore only this script's generated packaging edits, never any controller files.
git -C "$source_dir" restore pom.xml package.xml
for module in "${modules[@]}"; do
  git -C "$source_dir" restore "$module/pom.xml"
  if [[ -f "$source_dir/$module/src/main/assembly/package.xml" ]]; then
    git -C "$source_dir" restore "$module/src/main/assembly/package.xml"
    perl -0pi -e 's/<id><\/id>/<id>runtime<\/id>/g' "$source_dir/$module/src/main/assembly/package.xml"
  fi
  perl -0pi -e 's/<finalName>datax<\/finalName>/<finalName>datax<\/finalName><appendAssemblyId>false<\/appendAssemblyId>/g' "$source_dir/$module/pom.xml"
done
perl -0pi -e 's{<modules>.*?</modules>}{<modules><module>common</module><module>transformer</module><module>core</module><module>plugin-rdbms-util</module><module>mysqlreader</module><module>postgresqlwriter</module><module>streamreader</module><module>streamwriter</module></modules>}s; s{<artifactId>maven-assembly-plugin</artifactId>}{<artifactId>maven-assembly-plugin</artifactId><version>3.7.1</version>}' "$source_dir/pom.xml"
perl -0pi -e 's/<id><\/id>/<id>runtime<\/id>/g' "$source_dir/package.xml"
(cd "$source_dir" && mvn -B -DskipTests -Dmaven.test.skip=true package)
mkdir -p "$runtime_dir"
for module in core transformer mysqlreader postgresqlwriter streamreader streamwriter; do
  cp -R "$source_dir/$module/target/datax/." "$runtime_dir/"
done
cp "$source_dir/license.txt" "$source_dir/NOTICE" "$runtime_dir/"
printf '%s\n' "$revision" > "$runtime_dir/UPSTREAM_REVISION"
printf 'DataX runtime: %s\n' "$runtime_dir"
