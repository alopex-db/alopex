#!/usr/bin/env bash
# Keep measured output and arbitrary failures off the public Actions log.
set -euo pipefail
umask 077
if [ "$#" -lt 2 ]; then
  printf '::error::PARITY_STAGE_CODE_INVALID\n'
  exit 1
fi
code="$1"
shift
case "$code" in
  prepare_python|dependencies|build|contracts|differential|hnsw_dataset|scale_dataset|benchmark|sql_knn_cost|prepared_batch_throughput|gate|status|public_inventory)
    ;;
  *) printf '::error::PARITY_STAGE_CODE_INVALID\n'; exit 1 ;;
esac
if ! install -d -m 700 performance-debug 2>/dev/null; then
  printf '::error::PARITY_DIAGNOSTIC_DIRECTORY_FAILED\n'
  exit 1
fi
if ( "$@" > "performance-debug/${code}.log" 2>&1 ) 2>/dev/null; then
  exit 0
fi
printf '::error::PARITY_%s_FAILED\n' "$code"
exit 1
