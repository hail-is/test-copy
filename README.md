This is code to test in the sense of benchmarking the Hail copy code.
It is definitely rough but I think it points in a good direction.  I
found it quite useful.

It does four things:

 - It creates VMs in GCP and AWS to run the tests.

 - It creates data in a variety of configurations for the tests.

 - It runs the tests and (over)writes `results.json` with the results.

 - It cleans everything up.

You can do these all at once, or to support a development workflow,
you can do each of these independently.  In the course of use, you
might want to add additional commands for smaller steps you want to
isolate (e.g. a single test case instead of all of them).

Resources get prefixed with the profile ID so you can have multiple
benchmarks running at once.  You can set the profile with `--profile`.

To run this you will need:

 - gcloud to be authorized to account that has access to the 1-day
   bucket and the broad-ctsa project.  Your user account should work
   for this.

 - Default AWS credentials should be for the AWS broad-hail-bench
   account `test` user.  I find the AWS named profiles a useful
   feature when working with multiple accounts (like our account and
   the ODP account).  See:
   https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html.

 - The AWS "Key pair" named "default" must be saved in PEM format in
   ~/.ssh/aws-broad-hail-bench-default. If you didn't create the default key
   pair, then you need to delete the existing one and create a fresh one also
   named "default". Ideally, this tool would be parameterized by a key pair.

 - A Hail package must be installed for the `cleanup` command that uses
   AsyncFS.rmtree.

The setup for the tests is specified in `config.yaml`.  It has has
extensive tests: 5GB spread across 1, 20, or 40K files or 40GB in one
file, copied 6 ways: two and from GCS, S3 and the local file system.

`config.yaml` contains commands to create or delete VMs on GCP and
AWS.  On GCP, it uses a n2-standard-8 with an SSD, on Amazon the
closest instance type I could find, a m5d.2xlarge with an SSD.

A data configuration is a total size, the number of files to spread
the data across, and the number of directory levels to include
(directory names are hex digits so the branching factor is 16).

`test-copy.py` is the main entrypoint.

The `create-vms` command creates the VMs.

The `create-data` command creates data in the locations given in the
config file, which also specifies which VM to run them on.

The `test-copy` runs the tests given by the `cases` section of the
config file.  A case is a source, destination, and place to run the
test.  It writes the `results.json` file (and gives lots of output).
`test-copy` does not verify the integrity of the copied data.

The `cleanup` command deletes all the resources.

The tool pulls the Hail repo on the VMs when running the tests.  Which
repo/version to choose is determined by the `get` section of the
config file.
