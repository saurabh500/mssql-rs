# Dev environment setup for SSH linux

Run the `./install-deps.sh` This install the prereqs on Ubuntu

After this download the `msrustup` using the `install-msrustup.sh` script.

This will place the `msrustup` binary in the same folder as the scripts. 

Now run `./msrustup`. For Authentication, you need to use DeviceCode authentication, since the browser won't work in SSH. 

Restart the terminal. 


Now Cargo should be available in the PATH. 

From the root of the repo, run `cargo build`. This should succeed. 

