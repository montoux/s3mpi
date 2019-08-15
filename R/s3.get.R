#' Fetch an R object from an S3 path.
#'
#' @param s3key character. A full S3 s3key
#' @param bucket_location character. Usually \code{"US"}.
#' @param verbose logical. If \code{TRUE}, the \code{s3cmd}
#'    utility verbose flag will be set.
#' @param debug logical. If \code{TRUE}, the \code{s3cmd}
#'    utility debug flag will be set.
#' @param cache logical. If \code{TRUE}, an LRU in-memory cache will be referenced.
#' @param storage_format character. What format the object is stored in. Defaults to RDS.
#' @aliases s3.put
#' @return For \code{s3.get}, the R object stored in RDS format on S3 in the \code{path}.
#'    For \code{s3.put}, the system exit code from running the \code{s3cmd}
#'    command line tool to perform the upload.
s3.get <- function (s3key, bucket_location = "US", verbose = FALSE, debug = FALSE, cache = TRUE, storage_format = c("RDS", "CSV", "table", "XLSX", "datatable"), ...) {
  storage_format <- match.arg(storage_format)

  cache_id <- s3key

  if (!is.null(storage_format) && storage_format == 'XLSX') {
    arguments <- list(...)
    cache_id <- paste0(s3key, '#', arguments[["sheet"]])
  }

  # Helper function for fetching data from s3
  fetch <- function(s3key, storage_format, bucket_location, ...) {
    x.serialized <- tempfile()
    dir.create(dirname(x.serialized), showWarnings = FALSE, recursive = TRUE)
    ## We remove the file [when we exit the function](https://stat.ethz.ch/R-manual/R-patched/library/base/html/on.exit.html).
    on.exit(unlink(x.serialized), add = TRUE)

    if (file.exists(x.serialized)) {
      unlink(x.serialized, force = TRUE)
    }

    ## Run the s3cmd tool to fetch the file from S3.
    cmd <- s3cmd_get_command(s3key, x.serialized, bucket_location_to_flag(bucket_location), verbose, debug)
    status <- system2(s3cmd(), cmd)

    if (as.logical(status)) {
      warning("Nothing exists for key ", s3key)
      `attr<-`(`class<-`(data.frame(), c("s3mpi_error", status)), "key", s3key)
    } else {
      ## And then read it back in RDS format.
      load_from_file <- get(paste0("load_as_", storage_format))
      load_from_file(x.serialized, ...)
    }
  }

  ## Check for the path in the cache
  ## If it does not exist, create and return its entry.
  ## The `s3LRUcache` helper is defined in utils.R
  if (is.windows() || isTRUE(get_option("s3mpi.disable_lru_cache")) || !isTRUE(cache)) {
    ## We do not have awk, which we will need for the moment to
    ## extract the modified time of the S3 object.
    ans <- fetch(s3key, storage_format, bucket_location, ...)
  } else if (!s3LRUcache()$exists(cache_id)) {
    ans <- fetch(s3key, storage_format, bucket_location, ...)

    ## We store the value of the R object in a *least recently used cache*,
    ## expecting the user to not think about optimizing their code and
    ## call `s3read` with the same key multiple times in one session. With
    ## this approach, we keep the latest 10 object in RAM and do not have
    ## to reload them into memory unnecessarily--a wise time-space trade-off!
    tryCatch(s3LRUcache()$set(cache_id, ans), error = function(...) {
      warning("Failed to store object in LRU cache. Repeated calls to ",
              "s3read will not benefit from a performance speedup.")
    })
  } else {
    # Check time on s3LRUcache's copy
    last_cached <- s3LRUcache()$last_accessed(cache_id) # assumes a POSIXct object

    # Check time on s3 remote's copy using the `s3cmd info` command.
    if (use_legacy_api()) {
      s3.cmd <- paste("info ", s3key, "| head -n 3 | tail -n 1")
      result <- system2(s3cmd(), s3.cmd, stdout = TRUE, stderr = NULL)
      # The `s3cmd info` command produces the output
      # "    Last mod:  Tue, 16 Jun 2015 19:36:10 GMT"
      # in its third line, so we subset to the 20-39 index range
      # to extract "16 Jun 2015 19:36:10".
      result <- substring(result, 20, 39)
      last_updated <- strptime(result, format = "%d %b %Y %H:%M:%S", tz = "GMT")
    }
    else {
      s3.cmd <- paste0("s3 ls ", s3key)
      result <- system2(s3cmd(), s3.cmd, stdout = TRUE, stderr = NULL)
      # The `aws s3 ls` command produces the output
      # "    2019-07-30 11:59:58   ..."
      result <- substring(result, 0, 19)
      last_updated <- strptime(result, format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
    }

    if (last_updated > last_cached) {
      ans <- fetch(s3key, storage_format, bucket_location, ...)
      s3LRUcache()$set(cache_id, ans)
    } else {
      ans <- s3LRUcache()$get(cache_id)
    }
  }
  ans
}

s3cmd_get_command <- function(path, file, bucket_flag, verbose, debug) {
  if (use_legacy_api()) {
    paste("get", paste0('"', path, '"'), file,
          bucket_flag,
          if (verbose) "--verbose --progress" else "--no-progress",
          if (debug) "--debug" else "")
  } else {
    paste0("s3 cp ", path, " ", file)
  }
}

## Given an s3cmd path and a bucket location, will construct a flag
## argument for s3cmd.  If it looks like the s3cmd is actually
## pointing to an s4cmd, return empty string as s4cmd doesn't
## support bucket location.
bucket_location_to_flag <- function(bucket_location) {
  if (grepl("s4cmd", s3cmd())) {
    if (bucket_location != "US") {
        warning(paste0("Ignoring non-default bucket location ('",
                       bucket_location,
                       "') in s3mpi::s3.get since s4cmd was detected",
                       "-- this might be a little slower but is safe to ignore."));
    }
    return("")
  }
  return(paste("--bucket_location", bucket_location))
}

load_as_RDS <- function(filename, ...) {
  readRDS(filename, ...)
}

load_as_CSV <- function(filename, ...) {
  read.csv(filename, ..., stringsAsFactors = FALSE)
}

load_as_table <- function(filename, ...) {
  read.table(filename, ..., stringsAsFactors = FALSE)
}

load_as_datatable <- function(filename, ...) {
  fread(filename, ..., stringsAsFactors = FALSE)
}


load_as_XLSX <- function(filename, ...) {
  if (!requireNamespace("readxl", quietly = TRUE)) {
    stop("Package \"readxl\" needed for this function to work. Please install it.",
    call. = FALSE)
  }
  readxl::read_xlsx(filename, ...)
}

#' Printing for s3mpi errors.
#'
#' @param x ANY. R object to print.
#' @param ... additional objects to pass to print function.
#' @export
print.s3mpi_error <- function(x, ...)  {
  cat("Error reading from S3: key", crayon::white$bold(attr(x, "key")), "not found.\n")
}
