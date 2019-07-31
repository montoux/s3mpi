#' Determine whether object exists on S3.
#'
#' Test whether or not the given object exists at the
#' give S3 path.
#'
#' @param name string. Name of file to look for
#' @param path string. Path to file.  If missing, the entire s3 path must be provided in name.
#' @export
#' @examples \dontrun{
#' s3exists("my/key") # Will look in bucket given by getOption("s3mpi.path") or
#' from a system environment variable.
#'   # For example, if this option is "s3://mybucket/", then this query
#'   # will check for existence of the \code{s3://mybucket/my/key} S3 path.
#'
#' s3exists("my/key", "s3://anotherbucket/") # We can of course change the bucket.
#' }
s3exists <- function(name, path = s3path()) {
  s3key = create_s3key(path, name)
  results <- system2(s3cmd(), s3cmd_exists_command(s3key), stdout = TRUE)
  check_exists_results(name, results)
}

s3cmd_exists_command <- function(s3key) {
  if (use_legacy_api()) {
    paste("ls", s3key)
  } else {
    paste("s3", "ls", s3key)
  }
}


check_exists_results <- function(name, results) {
  ## We know that the key exists if a result was returned, i.e., the
  ## shown regex gives a match.
  if (use_legacy_api()) {
    matches <- grepl(paste0(name, "(/[0-9A-Za-z]+)*/?$"), results)
  } else {
    matches <- grepl(paste0(basename(name), "$"), results)
  }
  sum(matches) > 0
}
