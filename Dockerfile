# Dockerfile to build portable binary compatible with GLIBC 2.28+
# Uses Debian 10 (buster) with Python 3.9 - GLIBC 2.28 compatible with AlmaLinux 8
# Python base image is simpler and faster to build
# Note: Debian 10 (buster) is EOL, so we use archive.debian.org

FROM python:3.9-slim-buster

# Update sources to use Debian archive (buster is EOL)
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|https://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i 's|https://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    find /etc/apt/sources.list.d/ -name "*.list" -exec sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' {} \; 2>/dev/null || true && \
    find /etc/apt/sources.list.d/ -name "*.list" -exec sed -i 's|https://deb.debian.org/debian|http://archive.debian.org/debian|g' {} \; 2>/dev/null || true && \
    find /etc/apt/sources.list.d/ -name "*.list" -exec sed -i 's|http://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' {} \; 2>/dev/null || true && \
    find /etc/apt/sources.list.d/ -name "*.list" -exec sed -i 's|https://security.debian.org/debian-security|http://archive.debian.org/debian-security|g' {} \; 2>/dev/null || true

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        binutils \
        && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY ./script/apscheduler_migration.py /build/
COPY ./script/scheduler /build/scheduler

RUN pip install --no-cache-dir \
    "apscheduler>=3.10.0" \
    "pytz>=2021.1" \
    "pytz-deprecation-shim>=0.1.0" \
    "tzlocal>=3.0" \
    "tzdata>=2021.1" \
    "sqlalchemy>=1.4.0" \
    "python-dateutil>=2.8.0" \
    pyinstaller

# Add scheduler to Python path and verify it can be imported
RUN python -c "import scheduler.usecases.job; print('scheduler module import successful')"

RUN pyinstaller \
    --onefile \
    --name apscheduler_migration \
    --clean \
    --noconfirm \
    --paths=/build \
    --hidden-import=apscheduler \
    --hidden-import=apscheduler.triggers.combining \
    --hidden-import=apscheduler.triggers.cron \
    --hidden-import=apscheduler.triggers.date \
    --hidden-import=apscheduler.triggers.interval \
    --hidden-import=dateutil \
    --hidden-import=dateutil.parser \
    --hidden-import=pytz \
    --hidden-import=pytz_deprecation_shim \
    --hidden-import=tzlocal \
    --hidden-import=sqlite3 \
    --hidden-import=zoneinfo \
    --hidden-import=scheduler \
    --hidden-import=scheduler.usecases \
    --hidden-import=scheduler.usecases.job \
    --collect-submodules=scheduler \
    apscheduler_migration.py && \
    chmod +x /build/dist/apscheduler_migration


FROM scratch AS export
COPY --from=0 /build/dist/apscheduler_migration /output/apscheduler_migration