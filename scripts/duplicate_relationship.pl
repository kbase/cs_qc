#!/usr/bin/perl -w
use DBI;
use strict;
use warnings;

#
#	Find duplicate relationship records without knowing the structure ahead of time
#

my $ver = 4; 
my $db = "kbase_sapling_v";
my $user = '';
my $pass = '';
my $host = "db$ver.chicago.kbase.us";

my $dbh = DBI->connect("dbi:mysql:database=$db;host=$host",$user,$pass)
	or die "Connection Error: $DBI::errstr\n";

my $sql = "show tables";
 
my $sth = $dbh->prepare($sql);
$sth->execute or die "SQL Error: $DBI::errstr\n";

my @tables;
while (my $row = $sth->fetchrow_array) 
{
#	print "DEBUG: ROW = $row----\n";
	push(@tables,$row) unless ($row =~ /^\_/);
}

my $out = "./duplicate_relationship.dat";
open (OUT,">$out") || die "Did not create $out";

foreach my $table (sort(@tables))
{
#	print "DEBUG: TABLE=$table\n";
	next unless ($table gt 'IsFunctionalIn');
	$sql = "desc $table";
 
	$sth = $dbh->prepare($sql);
	$sth->execute or die "SQL Error: $DBI::errstr\n";

	my $from = 'N';
	my $to   = 'N';
	my $other = 'N';
	while (my @columns = $sth->fetchrow_array) 
	{
#		print "DEBUG: COLUMN0 = $columns[0] COLuMNS1=$columns[1]----\n";
		if ($columns[0] eq 'from_link')
		{
			$from = 'Y' ;
		}
		elsif ($columns[0] eq 'to_link')
		{
			$to   = 'Y' ;
		}
		else
		{
			$other   = 'Y' ;
		}
	}
#	if ($from eq 'Y' && $to eq 'Y' && $other eq 'N')
	if ($from eq 'Y' && $to eq 'Y' )
	{
		print OUT "TABLE=$table \n";
		print  "TABLE=$table \n";
		my $sql = "SELECT count(*) as cnt, from_link, to_link FROM $table GROUP BY to_link,from_link HAVING cnt > 1";
 
		my $sth = $dbh->prepare($sql);
		$sth->execute or die "SQL Error: $DBI::errstr\n";

		my $count = 0;
		while (my @results = $sth->fetchrow_array) 
		{
#			print OUT "@results\n";
#			print "@results\n";
			$count++;
		}
		print OUT "Number of duplicate rows = $count\n\n";
		print "Number of duplicate rows = $count\n\n";
	}
}
close OUT;
