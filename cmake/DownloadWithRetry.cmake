# DownloadWithRetry.cmake
#
# Provides download_with_retry(), a wrapper around file(DOWNLOAD) that retries
# transient HTTP failures and verifies an optional expected hash.
#
# Fetching release assets during configure is a common source of CI flakiness:
# many jobs pull the same asset from behind a single shared egress address, and
# a single HTTP 500 from the CDN is enough to fail a build leg that has nothing
# wrong with it. This helper bounds that failure mode in one place so every
# fetched dependency inherits the same behavior.
#
# The backoff defaults below are set from a measured incident rather than by
# guesswork. In CI run 30808644796 attempt 1 (2026-08-03), the DXC prebuilt
# fetch in build-linux-debug-gcc-x86_64 got an HTTP 500 at 11:15:48.39Z and
# failed 180ms later. The sibling build-linux-release-gcc-x86_64 job requested
# the *same URL* 7.1s after that and succeeded; another job succeeded 6m20s
# later. So the failure was per-request rather than a sustained network outage,
# and a retry a few seconds later lands after the CDN is serving again. Note
# also how cheap a wrong guess is: a failed attempt costs the ~180ms round trip,
# so three attempts cost at most ~15s of backoff, against a 10-30 minute
# fallback source build or a red build leg.

include_guard(GLOBAL)

# Download `url` to `output_path`, retrying transient failures, and report the
# outcome through the variable named by `out_error_var`: empty on success, or a
# human-readable message describing the last failure.
#
# The caller decides what a failure means. This function never raises a CMake
# error of its own, so a caller that has a fallback path (building a dependency
# from source, say) can take it, and a caller that does not can turn the message
# into a warning or a FATAL_ERROR itself.
#
# Retries cover any kind of failure, transient or permanent, up to MAX_ATTEMPTS:
# there is no reliable way to tell a retryable 500 from a permanent 404 here, so
# a genuinely missing asset pays the full backoff before giving up. That costs a
# few seconds on a path that is already going to fail.
#
# Options:
#   EXPECTED_HASH  <ALGO=digest> - verify the downloaded file against this hash
#   HTTPHEADER     <header>      - extra HTTP header, may be repeated
#   USERPWD        <user:pass>   - forwarded to file(DOWNLOAD)
#   NETRC          <level>       - forwarded to file(DOWNLOAD)
#   NETRC_FILE     <path>        - forwarded to file(DOWNLOAD)
#   TLS_VERIFY     <bool>        - forwarded to file(DOWNLOAD)
#   TLS_CAINFO     <path>        - forwarded to file(DOWNLOAD)
#   SHOW_PROGRESS                - forwarded to file(DOWNLOAD)
#   MAX_ATTEMPTS   <n>           - total attempts, including the first (default 3)
#   INACTIVITY_TIMEOUT <seconds> - abort an attempt after this long with no data
#                                  (default 60)
#   RETRY_DELAY    <seconds>     - base delay for the linear backoff between
#                                  attempts (default 5, giving 5s then 10s ...)
#
# The defaults are tuned for fetching release archives during configure; a call
# site fetching something small and latency-sensitive can shorten them, and one
# fetching something very large can raise the inactivity timeout.
#
# A note on EXPECTED_HASH: it is deliberately *not* forwarded to file(DOWNLOAD).
# When file(DOWNLOAD) is given both STATUS and EXPECTED_HASH and the transfer
# fails, it records a hard "cannot compute hash on failed download" CMake error
# in addition to returning the status. That error is deferred rather than
# immediate: the script keeps running, so the caller's status check and fallback
# both appear to work, but configure still fails at the end regardless. Retrying
# around such a call just raises the same error once per attempt. Verifying the
# hash here, after the transfer succeeded, keeps the integrity check while
# leaving the caller in control of what happens on failure.
function(download_with_retry url output_path out_error_var)
    set(options SHOW_PROGRESS)
    set(one_value_args
        EXPECTED_HASH
        MAX_ATTEMPTS
        INACTIVITY_TIMEOUT
        RETRY_DELAY
        USERPWD
        NETRC
        NETRC_FILE
        TLS_VERIFY
        TLS_CAINFO
    )
    set(multi_value_args HTTPHEADER)
    cmake_parse_arguments(
        arg
        "${options}"
        "${one_value_args}"
        "${multi_value_args}"
        ${ARGN}
    )

    if(NOT DEFINED arg_MAX_ATTEMPTS)
        set(arg_MAX_ATTEMPTS 3)
    endif()
    # A stalled connection has to fail before the retry below can get control
    # back, so bound it. INACTIVITY_TIMEOUT rather than TIMEOUT: it fires only
    # when no data is arriving, whereas a wall-clock limit would also abort a
    # slow but healthy transfer of a large archive.
    if(NOT DEFINED arg_INACTIVITY_TIMEOUT)
        set(arg_INACTIVITY_TIMEOUT 60)
    endif()
    # Linear backoff between attempts: 5s then 10s, with no wait after the last try.
    if(NOT DEFINED arg_RETRY_DELAY)
        set(arg_RETRY_DELAY 5)
    endif()

    # Forward the pass-through options that were actually supplied. Each is
    # only appended when set, so file(DOWNLOAD) keeps its own default for any
    # option the caller did not mention.
    set(download_args "")
    foreach(header IN LISTS arg_HTTPHEADER)
        list(APPEND download_args HTTPHEADER "${header}")
    endforeach()
    foreach(
        opt
        IN
        ITEMS USERPWD NETRC NETRC_FILE TLS_VERIFY TLS_CAINFO
    )
        if(DEFINED arg_${opt})
            list(APPEND download_args ${opt} "${arg_${opt}}")
        endif()
    endforeach()
    if(arg_SHOW_PROGRESS)
        list(APPEND download_args SHOW_PROGRESS)
    endif()

    set(last_error "")
    foreach(attempt RANGE 1 ${arg_MAX_ATTEMPTS})
        file(
            DOWNLOAD "${url}" "${output_path}"
            STATUS status
            INACTIVITY_TIMEOUT ${arg_INACTIVITY_TIMEOUT}
            ${download_args}
        )

        list(GET status 0 status_code)
        list(GET status 1 status_string)

        if(status_code EQUAL 0)
            if(NOT DEFINED arg_EXPECTED_HASH)
                set(${out_error_var} "" PARENT_SCOPE)
                return()
            endif()

            # EXPECTED_HASH is given as "ALGO=digest", matching the spelling
            # file(DOWNLOAD) and FetchContent's URL_HASH use, so call sites can
            # pass the same variable to either.
            string(REPLACE "=" ";" hash_parts "${arg_EXPECTED_HASH}")
            list(GET hash_parts 0 hash_algo)
            list(GET hash_parts 1 expected_digest)
            file(${hash_algo} "${output_path}" actual_digest)

            if(actual_digest STREQUAL expected_digest)
                set(${out_error_var} "" PARENT_SCOPE)
                return()
            endif()

            set(last_error
                "hash mismatch: expected ${hash_algo} ${expected_digest}, got ${actual_digest}"
            )
        else()
            set(last_error "${status_string} (status code ${status_code})")
        endif()

        # A failed download still leaves a file behind, and extracting an empty
        # archive succeeds silently, so the partial file has to be removed. Left
        # in place it would be mistaken for a good cached archive on every
        # subsequent configure, and the fetch would never recover. The same
        # applies to a file that downloaded cleanly but failed hash
        # verification.
        file(REMOVE "${output_path}")

        if(attempt LESS arg_MAX_ATTEMPTS)
            math(EXPR retry_delay "${arg_RETRY_DELAY} * ${attempt}")
            message(
                STATUS
                "Download of ${url} failed (${last_error}), retrying in ${retry_delay}s ..."
            )
            execute_process(COMMAND ${CMAKE_COMMAND} -E sleep ${retry_delay})
        endif()
    endforeach()

    set(${out_error_var} "${last_error}" PARENT_SCOPE)
endfunction()
