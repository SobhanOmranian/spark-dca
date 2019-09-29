FILE_NAME=`ls $RESULT_HOME/*.dca | head -n 1 | egrep 'app-[0-9]+-[0-9]+' | sed -r "s/.+\/(.+)\..+/\1/" | cut -f1 -d"#"`
cat $RESULT_HOME/*.dca | awk '{if (NR>1 && $0 ~/executorId/) { next } else print}' > $RESULT_HOME/combined_${FILE_NAME}.dca
cp $RESULT_HOME/combined_${FILE_NAME}.dca $RESULT_HOME/combined.dca
