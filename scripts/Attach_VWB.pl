#!/usr/bin/perl -w

use strict;

die "Usage: $0 <vcf> <tsv>" unless (@ARGV == 2);

my ($chr_i, $pos_i, $ref_i, $alt_i, $id_i) = ("", "", "", "", "");
my @info_to_add = ();
my @info_to_add_i = ();
my %hash = ();
my $header_to_add = "";

open(T, "$ARGV[1]") || die "$!";
while (<T>) {
	s/[\n\r]+$//;
	my @t = split(/\t/, $_, -1);
	if (/^chr/) { # header line
		for (my $i = 0; $i < @t; $i ++) {
			if ($t[$i] eq "chromosome" ) {
				$chr_i = $i;
			} elsif ($t[$i] eq "start") {
				$pos_i = $i;
			} elsif ($t[$i] eq "reference") {
				$ref_i = $i;
			} elsif ($t[$i] eq "alternate") {
				$alt_i = $i;
			} elsif ($t[$i] =~/^(?:end|name|qual|filters|splitFromMultiAllelic|INFO_|genotypes)/) {
				# skip
			} else {
				push(@info_to_add_i, $i);
				push(@info_to_add, "VWB_$t[$i]");
				$header_to_add .= "##INFO=<ID=VWB_$t[$i],Number=.,Type=String,Description=\"Annotation added via Variant WorkBench.\">\n";
			}
		}
	} else {
		my $key = "chr$t[$chr_i]\t$t[$pos_i]\t$t[$ref_i]\t$t[$alt_i]";
		my $value = "";
		for (my $i = 0; $i < @info_to_add_i; $i ++) {
			if (defined $t[$info_to_add_i[$i]] && $t[$info_to_add_i[$i]] ne "") {
				$value .= ";$info_to_add[$i]=$t[$info_to_add_i[$i]]";
			}
		}
		$hash{$key} = $value;
	}
}
close(T);

($chr_i, $pos_i, $ref_i, $alt_i, $id_i) = ("", "", "", "", "");
my ($info_i, $format_i) = ("", "");

open(V, "$ARGV[0]") || die "$!";
while (<V>) {
	s/[\n\r]+$//;
	my @t = split(/\t/);
	if (/^\#\#/) {
		print "$_\n";
	} elsif (/^\#CHR/) {
		print "$header_to_add$_\n";
		for (my $i = 0; $i < @t; $i ++) {
			if ($t[$i] =~/^\#CHR/) {
				$chr_i = $i;
			} elsif ($t[$i] eq "POS") {
				$pos_i = $i;
			} elsif ($t[$i] eq "REF") {
				$ref_i = $i;
			} elsif ($t[$i] eq "ALT") {
				$alt_i = $i;
			} elsif ($t[$i] eq "ID") {
				$id_i = $i;
			} elsif ($t[$i] eq "INFO") {
				$info_i = $i;
			} elsif ($t[$i] eq "FORMAT") {
				$format_i = $i;
			}
		}
	} else {
		# my $key = "$t[$chr_i]\t$t[$pos_i]\t$t[$id_i]\t$t[$ref_i]\t$t[$alt_i]";
		my $key = "$t[$chr_i]\t$t[$pos_i]\t$t[$ref_i]\t$t[$alt_i]";
		print join("\t", @t[0 .. $info_i]), $hash{$key}, "\t", join("\t", @t[$format_i .. scalar(@t) - 1]), "\n";
	}
}
close(V);

