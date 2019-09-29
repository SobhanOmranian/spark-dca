sub report {
    my (@values) = @_;
    print("\n\n\n");
    print("===============\n");
    for(my $i=0; $i < @values; $i++) {
        my $val = $values[$i];
        print "Run [${i}] = ${val} ms\n";
    }
  my $n = scalar @values;
  my $avg = sum(@values)/$n;
  my $min = min(@values);
  my $max = max(@values);
  my $std_dev = ($min == $max) ? 0 : sqrt(sum(map {($_ - $avg) ** 2} @values) / $n);

  print "\n\n Average = ${avg}";
  print "\n Standard Deviation = ${std_dev}\n\n";
}

1;
