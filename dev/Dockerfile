# Use Ubuntu 22.04 as the base image
FROM mcr.microsoft.com/mirror/docker/library/ubuntu:22.04


# Define a build-time argument for the Rust version
ARG RUST_VERSION="ms-1.84"

# Set the argument as an environment variable so it persists
ENV RUST_VERSION=$RUST_VERSION

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install prerequisites
RUN apt update && apt install -y \
    curl \
    wget \
    jq \
    unzip \
    build-essential \
    pkg-config \
    libssl-dev \
    lsb-release \
    python-is-python3 \
    sudo \
    python3.10-venv \
    && wget -q https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && apt update \
    && apt install -y azureauth \
    && rm -f packages-microsoft-prod.deb

# Switch to the new user
COPY ./install-msrustup.sh /tmp/install-msrustup.sh

RUN --mount=type=secret,id=MSRUSTUP_ACCESS_TOKEN,env=MSRUSTUP_ACCESS_TOKEN \
    # Read the secret from the mounted location and set it as an environment variable
    # The environment variable is used by the install-msrustup.sh script
    # to authenticate with the feed to download the msrustup binary.
    set -eux; \
    # export MSRUSTUP_ACCESS_TOKEN=$(cat /run/secrets/MSRUSTUP_ACCESS_TOKEN) && \
    # Download and install msrustup
    # The install-msrustup.sh script uses the MSRUSTUP_ACCESS_TOKEN environment variable
    # and the script was downloaded from  
    # https://aka.ms/install-msrustup.sh
    /tmp/install-msrustup.sh  && \
    # Pre-install the toolchain.
    ./msrustup toolchain install $RUST_VERSION && \
    # Reload the bashrc to make the changes effective
    # . $HOME/.bashrc && \
    # These plugins take time to install, hence baking them
    # into the image itself.
    # The drawback is that the plugins will need to be updated manually if needed.
    # TODO: The build inside the container could initiate a plugin update.
    export PATH="$HOME/.cargo/bin:$PATH" && \
    /root/.cargo/bin/cargo install cargo-llvm-cov cargo-nextest maturin

# Add ~/.cargo/bin to PATH
ENV PATH="/home/tdsxdev/.cargo/bin:${PATH}"

# Set working directory to user's home
WORKDIR /src

CMD ["tail", "-f", "/dev/null"]
