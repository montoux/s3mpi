#' @param x ANY. R object to store to S3.
#' @param name character.
#' @param check_exists logical. Whether or not to check if an object already exists at the specificed location.
#' @param num_retries numeric. the number of times to retry uploading.
#' @param backoff numeric. Vector, with each element in seconds, describing the
#'   exponential backoff to be used in conjunction with the num_retries argument.
#'   Number of elements must equal num_retries. Defaults to 4, 8, 16, 32, etc.
#' @param max_backoff numeric. Number describing the maximum seconds s3mpi will sleep
#'   prior to retrying an upload. Defaults to 128 seconds.
#' @param storage_format character. What format to store files in. Defaults to RDS.
#' @param row.names logical. Whether or not to write row names when writing CSV's or tables.
#' @param ... additional arguments to pass the the saving function.
#' @rdname s3.get
s3.put <- function (x, path, name, bucket_location = "US",
                    debug = FALSE, check_exists = TRUE,
                    num_retries = get_option("s3mpi.num_retries", 0), backoff = 2 ^ seq(2, num_retries + 1),
                    max_backoff = 128, storage_format = c("RDS", "CSV", "table", "file"), row.names = FALSE, ...) {
  storage_format <- match.arg(storage_format)

  if (is.data.frame(x) && storage_format %in% c("CSV, table")) {
    stop("You can't store an object in ", storage_format," format if it isn't a data.frame.")
  }

  s3key <- create_s3key(path, name)

  ## Ensure backoff vector has correct number of elements and is capped
  if (num_retries > 0) {
    if (length(backoff) != num_retries) {
      stop("Your backoff vector length must match the number of retries.")
    }
    backoff <- pmin(backoff, max_backoff)
  }

  if (storage_format == "file") {
    x.serialized = x
  }
  else {
    ## We create a temporary file, *write* the R object to the file, and then
    ## upload that file to S3. This magic works thanks to R's fantastic
    ## support for [arbitrary serialization](https://stat.ethz.ch/R-manual/R-patched/library/base/html/readRDS.html)
    ## (including closures!).

    x.serialized <- tempfile();
    dir.create(dirname(x.serialized), showWarnings = FALSE, recursive = TRUE)
    on.exit(unlink(x.serialized, force = TRUE), add = TRUE)
    save_to_file <- get(paste0("save_as_", storage_format))
    save_to_file(x, x.serialized, row.names, ...)
  }

  cmd <- s3cmd_put_command(s3key, x.serialized, bucket_location_to_flag(bucket_location), debug)
  run_system_put(path, name, cmd, check_exists, num_retries, backoff)
}

run_system_put <- function(path, name, s3.cmd, check_exists, num_retries, backoff) {
  ret <- system2(s3cmd(), s3.cmd)
  if (isTRUE(check_exists) && !s3exists(name, path)) {
    if (num_retries > 0) {
      Sys.sleep(backoff[length(backoff) - num_retries + 1])
      Recall(path = path, name = name, s3.cmd = s3.cmd,
             check_exists = check_exists,
             num_retries = num_retries - 1, backoff = backoff)
    } else {
      stop("Object could not be successfully stored.")
    }
  } else {
    ret
  }
}

s3cmd_put_command <- function(s3key, file, bucket_flag, debug) {
  if (use_legacy_api()) {
    paste("put", file, paste0('"', s3key, '"'),
          bucket_flag, ifelse(debug, "--debug", ""), "--force")
  } else {
    paste("s3 cp", file, s3key)
  }
}

save_as_RDS <- function(x, filename, ...) {
  saveRDS(x, filename, ...)
}


save_as_CSV <- function(x, filename, row.names, ...) {
  write.csv(x, filename, row.names = row.names, ...)
}

save_as_table <- function(x, filename, row.names, ...) {
  write.table(x, filename, row.names = row.names, ...)
}
