#!/usr/bin/perl -w
use DBI;
use Getopt::Long; 
use Data::Dumper;
use strict;
use warnings;

my $db_name = undef; 
my $db_user = undef; 
my $db_pwd = undef; 
 
my $usage = "This command requires the db_name, db_user, db_pwd\n". 
    "The DB parameters need to be in single quotes.  \n". 
    "Example Call : \n". 
    "perl get_table_sizes.pl -db_name='kbase_sapling_v4:db4.chicago.kbase.us' -db_user='YOUR_DB_USER' -db_pwd='YOUR_DB_PWD'\n"; 
(GetOptions('db_name=s' => \$db_name, 
            'db_user=s' => \$db_user, 
            'db_pwd=s' => \$db_pwd 
) 
 && @ARGV == 0) || die $usage; 
die $usage if ((!defined $db_user) || (!defined $db_pwd) || (!defined $db_name)); 
 
my $full_db_name = 'DBI:mysql:'.$db_name; 
my $dbh = DBI->connect($full_db_name,$db_user, $db_pwd, { RaiseError => 1, ShowErrorStatement => 1 } ); 

my ($db,$dummy) = split(':',$db_name);
#
#	Get a master list of all the tables and their columns
#	Want to Identify tables as Relationship or Entity
#
my $sql = "select * from information_schema.columns where table_schema = '$db' order by table_name,ordinal_position";

my $sth = $dbh->prepare($sql);
$sth->execute or die "SQL Error: $DBI::errstr\n";

#
#	Two outputs
#	1.	Size for each table and db, date, relationship
#	2.	A list of empty entity and relationship tables
#
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
$year += 1900; 

my $out = "./test_table_size_".$db_name."_".$year."_".$mon."_".$mday.".txt";
open (OUT1,">$out") || die "Did not create $out";
 
$out = "./table_empty.txt";
open (OUT2,">$out") || die "Did not create $out";

#	Loop through the tables and columns
#	For columns that can't be null, create a test for an empty string
#
my %Tables;
while (my @row = $sth->fetchrow_array) 
{
	my $table  = $row[2];
	my $column = $row[3];
	$Tables{$table} = 'Unknown' unless (exists $Tables{$table});
#	print "DEBUG: TABLE=$table CoLUMN=$column\n";
	if ($column eq 'from_link' || $column eq 'to_link')
	{
		$Tables{$table} = 'Relationship';
	}
	elsif ($column eq 'id' )
	{
		$Tables{$table} = 'Entity';
	}
}

foreach my $table (sort(keys(%Tables)))
{
	print "DEBUG: TABLE=$table\n";
	$sql = "select count(*) from $table";
	$sql = "select '".$table."_count' query_name, NOW(), database(), count(*) from ".$table; 
	$sth = $dbh->prepare($sql);
	$sth->execute or die "SQL Error: $DBI::errstr\n";

	while (my ($query_name, $date, $db, $count) = $sth->fetchrow_array())
	{
	    print OUT1 "$table\t$query_name\t$date\t$db\t$Tables{$table}\t$count\n";
	    print OUT2  "$table\t$Tables{$table}\n" if ($count == 0);
	}
}


my $sql = "select 'CDS_Feature_count' query_name, NOW(), database(), count(*) from Feature where feature_type = 'CDS'";
$sth = $dbh->prepare($sql);
$sth->execute or die "SQL Error: $DBI::errstr\n"; 
 
while (my ($query_name, $date, $db, $count) = $sth->fetchrow_array()) 
{ 
    print OUT1 "Feature_CDS_count\t$query_name\t$date\t$db\tCustom\t$count\n";
}

close OUT1;
close OUT2;
