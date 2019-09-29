    BEGIN {
        count = 0
        threshold = 7
        output = ""
    }

    {
        if ($0 == "" || NR == 1) {
            #print("empty or first line")
            next
        }
        buffer[count] = $0
        #print(buffer[0])
        count++
        #print(sprintf("[%s]: %s",count,$0))
        if (count == threshold) {
            for (j=1; j<=threshold;j++) {
                output = sprintf("%s^%s", output, buffer[j])
            }
            #for (i in buffer) {
                #output = sprintf("%s^%s", output, buffer[i])
            #}
            
            print(output)
            
            #fflush(stdout);
            output = ""
            count = 0
            delete buffer
        }
    }
