#!/usr/bin/env bash

measure_time() {
  local cmd="$1"
  echo "Uruchamiam: $cmd"
  local start=$(date +%s.%N)

  eval "$cmd"
  local end=$(date +%s.%N)

  local elapsed=$(echo "$end - $start" | bc)
  echo "Czas wykonania: ${elapsed}s"
  echo "-----------------------------"
}

measure_time "fqc -q ERR194147.fastq > report.html"

if [[ -d "../venv" ]]; then
  echo "Aktywacja venv z katalogu: $VENV_DIR"
  source "../venv/bin/activate"
else
  echo "Błąd: katalog '../venv' nie istnieje. Sprawdź ścieżkę do venv."
  exit 1
fi

measure_time "python main.py"

deactivate
