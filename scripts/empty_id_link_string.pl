#!/usr/bin/perl -w
#
#	Look for id, from_link, and to_link that are empty strings
#	Since these are used as PRIMARY IDs, they should alomost never be empty
#

use DBI;
use strict;
use warnings;

#
#	Database connection
#
my $ver = 4; 
my $db = "kbase_sapling_v$ver";
my $user = '';
my $pass = '';
my $host = "db$ver.chicago.kbase.us";

my $dbh = DBI->connect("dbi:mysql:database=$db;host=$host",$user,$pass)
	or die "Connection Error: $DBI::errstr\n";

#
#	Define output file
#
my $out = "./empty_id_link_string.v$ver.dat";
open (OUT,">$out") || die "Did not create $out";
print OUT "TABLE\tCOLUMN\tCOUNT\n";

#
#	Get a master list of all the tables and their columns
#
my $sql = "select * from information_schema.columns where table_schema = '$db' order by table_name,ordinal_position";
 
my $sth = $dbh->prepare($sql);
$sth->execute or die "SQL Error: $DBI::errstr\n";

#	Loop through the tables and columns
#	For columns that can't be null, create a test for an empty string
#
while (my @row = $sth->fetchrow_array) 
{
	my $table  = $row[2];
	my $column = $row[3];
#	print "DEBUG: TABLE=$table CoLUMN=$column\n";

	next unless ($column eq 'id' || $column eq 'from_link' || $column eq 'to_link');

	my $sql = "SELECT count(*) as cnt, $column FROM $table WHERE $column = '' GROUP BY $column";

	my $sth = $dbh->prepare($sql);
	$sth->execute or die "SQL Error: $DBI::errstr\n";

	my $count = 0;
	while (my @results = $sth->fetchrow_array) 
	{
		$count = $results[0];
	}
	print OUT "$table\t$column\t$count\n"  if ($count > 0);
	print "TABLE $table\tEmpty rows for $column = $count\n" if ($count > 0);
}
close OUT;
