#!/bin/bash
if [[ "$1" == "" ]] || [[ "$1" == "-h" ]]; then
  echo "GDPRBench workload config generator"
  echo "Usage: ./gen-loads.sh <output_directory> <user count> <record count> <op count>"
  echo "After usage, you can interactively specify your loads several times:"
  echo " 1. Enter the base workload filename, regex is allowed."
  echo "    e.g. pelton_delete_pk, pelton_select_*, *"
  echo " 2. Enter <usr count>, then <record count>, then <op count> to override"
  echo "    these parameters for this load. If any of these values are left empty,"
  echo "    by immediately hitting ENTER without typing in anything, the corresponding"
  echo "    global value used as a command line argument when the script is started"
  echo "    will be used."
  echo ""
  echo "Sample Usage: "
  echo "./gen-loads.sh user1 1 100 100"
  echo "> Provide input load names (can use regex): "
  echo "*"
  echo "> Override user count? "
  echo ""
  echo "> Override record count? "
  echo ""
  echo "> Override operation count? "
  echo ""
  echo "Processing <files...>"
  echo "> Provide input load names (can use regex, ctrl-D to stop): "
  echo "^D"
  echo "exit!"
  exit 0
fi

OUTPUT_DIR=$1
DEFAULT_USRCOUNT=$2
DEFAULT_RECORDCOUNT=$3
DEFAULT_OPCOUNT=$4

mkdir -p $OUTPUT_DIR

# Read files.
echo "> Provide input load names (can use regex): "
while read -r input; do
  if [[ "$input" == "*" ]]; then
    input=".*"
  fi

  echo "> Override user count? "
  read -r USRCOUNT;
  if [[ "$USRCOUNT" == "" ]]; then
    USRCOUNT=$DEFAULT_USRCOUNT
  fi

  echo "> Override record count? "
  read -r RECORDCOUNT;
  if [[ "$RECORDCOUNT" == "" ]]; then
    RECORDCOUNT=$DEFAULT_RECORDCOUNT
  fi

  echo "> Override operation count? "
  read -r OPCOUNT;
  if [[ "$OPCOUNT" == "" ]]; then
    OPCOUNT=$DEFAULT_OPCOUNT
  fi

  for f in $(ls . | grep "$input"); do
    if [[ "$f" == "gen-loads.sh" ]] || [ -d "$f" ]; then
      continue
    fi

    echo "Processing $f"
    of="$OUTPUT_DIR/$f"
    cp "$f" "$of"
    sed -i "s+usrcount=[0-9]*+usrcount=$USRCOUNT+g" "$of"
    sed -i "s+recordcount=[0-9]*+recordcount=$RECORDCOUNT+g" "$of"
    sed -i "s+operationcount=[0-9]*+operationcount=$OPCOUNT+g" "$of"
  done

  echo "> Provide input load names (can use regex, ctrl-D to stop): "  
done

echo "exit!"
