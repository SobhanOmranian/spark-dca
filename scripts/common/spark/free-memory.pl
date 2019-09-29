#!/usr/bin/perl
#
#
my $base_dir = "/home/omranian/spark/terasort-script";

if (@ARGV != 1) {
    print("You need to provide me the socket argument!\n");
    exit;
}

my ($socketArg) = @ARGV;
if($socketArg != 0 and $socketArg != 1) {
    print("Wrong socket@\n");
    exit;
}
my $numactlCmd = "numactl --cpunodebind=${socketArg} --membind=${socketArg}";
my $cmd = "${numactlCmd} ${base_dir}/allocate-memory.pl";

print("\n\n======== Clearing Buffer Cache on Socket ${socketArg} ==========\n\n ");

my $howMany = 15;

for (my $i; $i < $howMany; $i++) {
    $finalCmd .= "$cmd & ";
}

my $threshold = 2000;
my $currentFilePages = getFilePages($socketArg);
print("\nCurrent file pages = ${currentFilePages}\n");
if($currentFilePages <= $threshold) {
    print("\nThe socket is already clean! exiting...\n");
    exit;
}
system("( $finalCmd )");
while (1)
{
    my $socketFilePages = 10000;
    my $n0FilePages = 10000;
    my $n1FilePages = 10000;
    $socketFilePages = getFilePages($socketArg);
    if($socketFilePages <= $threshold) {
        print("\n\nBuffer cache of socket ${socketArg} has been cleared! stopping...\n\n");
        last;
    }
    else {
        print("Buffer cache of socket ${socketArg} -> [${socketFilePages}] is still not empty, continuing...\n")
    }
    sleep(3);
}

sub getFilePages {
    my ($socket) = @_;

    open my $pipe, "numastat -m -n |" or die("failed");
    my $socketFilePages = 10000;
    while (my $line = <$pipe>) {
        chomp($line);
        #print("${line} Line finished !\n");
        if($line =~ /FilePages\s*(\d+\.\d+)\s*(\d+\.\d+)\s*(\d+\.\d+)\s*/ ) {
            #print("${line}\n");
            if ($socket == 0) {
                $socketFilePages = $1;
            }
            else {
                $socketFilePages = $2;
            }
            my $n0FilePages = $1;
            my $n1FilePages = $2;
            #print("Node 0: ${n0FilePages}\n");
            #print("Node 1: ${n1FilePages}\n");
        }
    }
    return $socketFilePages;
}

system("pkill allocate-memory")
