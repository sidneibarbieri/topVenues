# Null-coalescing operator
`%||%` <- function(x, y) ifelse(is.null(x), y, x)

# Generic extract function
extract <- function(extractor, ...) UseMethod("extract")

#' Base class for abstract extraction.
#' @return An object of class AbstractExtractor.
AbstractExtractor <- function() {
  structure(list(), class = "AbstractExtractor")
}

#' Extract method for AbstractExtractor.
#' @param extractor An AbstractExtractor object.
#' @param url URL of the paper.
#' @param doi DOI of the paper.
#' @param collector A Collector object.
#' @return Abstract text or NA.
extract.AbstractExtractor <- function(extractor, url, doi, collector) {
  stop("Method not implemented")
}

#' Clean abstract text by removing authors and footers.
#' @param text Raw abstract text.
#' @return Cleaned text.
clean_abstract <- function(text) {
  text <- gsub("^([A-Z][a-z]+\\s)+\\([^)]+\\)[,:]?\\s*", "", text)
  text <- gsub("USENIX is committed to Open Access.*", "", text, ignore.case = TRUE)
  text <- gsub("\\s+", " ", text)
  trimws(text)
}

#' USENIX extractor class.
#' @return An object of class USENIXExtractor.
USENIXExtractor <- function() {
  structure(list(), class = c("USENIXExtractor", "AbstractExtractor"))
}

#' Extract method for USENIXExtractor.
#' @inheritParams extract.AbstractExtractor
#' @return Abstract text or NA.
extract.USENIXExtractor <- function(extractor, url, doi, collector) {
  current_agent <- sample(collector$user_agents, 1)
  xidel_args <- c(
    url,
    sprintf("--user-agent=%s", current_agent),
    '--header=Accept: text/html,application/xhtml+xml,application/xml;q=0.9',
    "-se"
  )
  # Primary XPath
  cmd <- c(xidel_args, 'normalize-space(((//main//section)[1]//p)[2])')
  output <- tryCatch(processx::run("xidel", cmd, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output) && nchar(output) >= 100) return(clean_abstract(output))
  # Fallback 1
  cmd <- c(xidel_args, 'normalize-space(//div[contains(@class,"field-name-field-paper-description")]//p)')
  output <- tryCatch(processx::run("xidel", cmd, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output) && nchar(output) >= 100) return(clean_abstract(output))
  # Fallback 2
  cmd <- c(xidel_args, 'normalize-space(string-join(((//main//section)[1]//p)[position() > 1 and position() < last()], " "))')
  output <- tryCatch(processx::run("xidel", cmd, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output) && nchar(output) >= 100) return(clean_abstract(output))
  NA
}

#' NDSS extractor class.
#' @return An object of class NDSSExtractor.
NDSSExtractor <- function() {
  structure(list(), class = c("NDSSExtractor", "AbstractExtractor"))
}

#' Extract method for NDSSExtractor.
#' @inheritParams extract.AbstractExtractor
#' @return Abstract text or NA.
extract.NDSSExtractor <- function(extractor, url, doi, collector) {
  current_agent <- sample(collector$user_agents, 1)
  xidel_args <- c(
    url,
    sprintf("--user-agent=%s", current_agent),
    '--header=Accept: text/html,application/xhtml+xml,application/xml;q=0.9',
    "-se"
  )
  # Primary XPath
  cmd <- c(xidel_args, 'normalize-space(string-join((//div[@class="paper-data"]//p)[position() > 1], " "))')
  output <- tryCatch(processx::run("xidel", cmd, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output) && nchar(output) >= 100) return(clean_abstract(output))
  # Fallback 1
  cmd <- c(xidel_args, 'normalize-space(string-join(//div[@id="abstract"]//p, " "))')
  output <- tryCatch(processx::run("xidel", cmd, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output) && nchar(output) >= 100) return(clean_abstract(output))
  # Fallback 2
  cmd <- c(xidel_args, 'normalize-space(string-join(//section[contains(@class,"abstract")]//p, " "))')
  output <- tryCatch(processx::run("xidel", cmd, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output) && nchar(output) >= 100) return(clean_abstract(output))
  NA
}

#' IEEE extractor class.
#' @return An object of class IEEEExtractor.
IEEEExtractor <- function() {
  structure(list(), class = c("IEEEExtractor", "AbstractExtractor"))
}

#' Extract method for IEEEExtractor.
#' @param extractor An IEEEExtractor object.
#' @param url URL of the paper.
#' @param doi DOI of the paper.
#' @param collector A Collector object.
#' @return Abstract text or NA.
extract.IEEEExtractor <- function(extractor, url, doi, collector) {
  current_agent <- sample(collector$user_agents, 1)
  base_args <- c(
    url,
    sprintf("--user-agent=%s", current_agent),
    '--header=Accept: text/html,application/xhtml+xml,application/xml;q=0.9',
    "-se"
  )
  
  primary_cmd <- c(base_args, 'string(//script[contains(.,"xplGlobal.document.metadata")])')
  output <- tryCatch(processx::run("xidel", primary_cmd, timeout = 30)$stdout, error = function(e) NULL)
  
  if (!is.null(output) && nchar(output) > 0) {
    matches <- regmatches(output, gregexpr('"abstract"\\s*:\\s*"([^"]+)"', output))[[1]]
    if (length(matches) > 0) {
      abs <- gsub('"abstract"\\s*:\\s*"', '', matches[1])
      abs <- gsub('"$', '', abs)
      abs <- gsub('\\"', '"', abs)
      abs <- gsub('\\\\n', ' ', abs)
      abs <- gsub("\\s+", " ", abs)
      abs <- trimws(abs)
      if (nchar(abs) >= 100 && !tolower(abs) %in% c("true", "false")) {
        return(clean_abstract(abs))
      }
    }
  }
  
  fallback_cmd <- c(base_args, 'string(//script[contains(.,"xplGlobal.document.metadata")])')
  raw_output <- tryCatch(processx::run("xidel", fallback_cmd, timeout = 30)$stdout, error = function(e) NULL)
  
  if (!is.null(raw_output) && grepl("xplGlobal.document.metadata", raw_output)) {
    line <- regmatches(raw_output, regexpr('xplGlobal.document.metadata\\s*=\\s*\\{.+?\\};', raw_output))
    if (length(line) > 0) {
      json_str <- sub('xplGlobal.document.metadata\\s*=\\s*', '', line)
      json_str <- sub(';$', '', json_str)
      metadata <- tryCatch(jsonlite::fromJSON(json_str), error = function(e) NULL)
      if (!is.null(metadata$abstract)) {
        abs <- metadata$abstract
        abs <- gsub('\\"', '"', abs)
        abs <- gsub('\\\\n', ' ', abs)
        abs <- gsub("\\s+", " ", abs)
        abs <- trimws(abs)
        if (nchar(abs) >= 100 && !tolower(abs) %in% c("true", "false")) {
          return(clean_abstract(abs))
        }
      }
    }
  }
  
  return(NA)
}

#' ACM extractor class.
#' @return An object of class ACMExtractor.
ACMExtractor <- function() {
  structure(list(), class = c("ACMExtractor", "AbstractExtractor"))
}

#' Extract method for ACMExtractor.
#' @inheritParams extract.AbstractExtractor
#' @return Abstract text or NA.
extract.ACMExtractor <- function(extractor, url, doi, collector) {
  failure_count <- collector$acm_failure_counts[[url]] %||% 0
  if (failure_count >= collector$acm_failure_threshold) return(NA)
  current_agent <- sample(collector$user_agents, 1)
  xidel_args <- c(
    url,
    sprintf("--user-agent=%s", current_agent),
    '--header=Accept: text/html,application/xhtml+xml,application/xml;q=0.9',
    "-se",
    'normalize-space(string(//*[@id="abstract"]))'
  )
  output <- tryCatch(processx::run("xidel", xidel_args, timeout = 30)$stdout, error = function(e) NULL)
  if (!is.null(output)) {
    output <- paste(output, collapse = " ")
    output <- gsub("\\s+", " ", output)
    output <- trimws(output)
    output <- sub("^Abstract\\s*[:\\?\\-]?\\s*", "", output, ignore.case = TRUE)
    if (nchar(output) >= 100) return(output)
  }
  NA
}

#' Collector class for managing data collection.
#' @param base_dir Base directory (default: COLLECTOR_DIR env var or current dir).
#' @return Collector object.
Collector <- function(base_dir = Sys.getenv("COLLECTOR_DIR", getwd())) {
  stopifnot(is.character(base_dir), dir.exists(base_dir))
  structure(list(
    base_dir = base_dir,
    top4_dir = file.path(base_dir, "top4"),
    data_dir = file.path(base_dir, "top4", "data"),
    log_dir = file.path(base_dir, "top4", "log"),
    json_dir = file.path(base_dir, "top4", "json"),
    log_file = file.path(base_dir, "top4", "log", "download_log.csv"),
    abstract_log = file.path(base_dir, "top4", "log", "abstract_log.csv"),
    master_file = file.path(base_dir, "top4", "data", "master_dataset.rds"),
    csv_file = file.path(base_dir, "top4", "data", "master_dataset.csv"),
    events = c("ccs", "asiaccs", "uss", "ndss", "sp", "eurosp", "hotnets", "sacmat"),
    years = 2019:2026,
    user_agents = c(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/129.0.2792.79 Safari/537.36"
    ),
    headers = c(
      "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
      "Accept-Language" = "en-US,en;q=0.5",
      "Connection" = "keep-alive",
      "Cache-Control" = "no-cache"
    ),
    log_df = NULL,
    master_df = NULL,
    acm_blocked_until = Sys.time(),
    acm_backoff = 60,
    acm_max_backoff = 600,
    request_interval = 5, # Base delay in seconds for non-ACM
    acm_wait_min = 60, # 1 minute for ACM requests
    acm_wait_max = 300, # 5 minutes for ACM requests
    acm_failure_threshold = 3, # Skip ACM URL after 3 consecutive 403s
    acm_failure_counts = list(),
    last_was_acm = FALSE, # Track if last processed paper was ACM
    batch_size = 10 # Process 10 papers per batch
  ), class = "Collector")
}

#' Setup collector environment.
#' @param collector Collector object.
#' @return Invisible collector.
setup.Collector <- function(collector) {
  stopifnot(inherits(collector, "Collector"))
  setwd(collector$base_dir)
  dirs <- c(collector$top4_dir, collector$data_dir, collector$log_dir, collector$json_dir)
  lapply(dirs, function(dir) {
    if (!dir.exists(dir)) dir.create(dir, recursive = TRUE)
  })
  required_pkgs <- c("httr", "rvest", "jsonlite", "lubridate", "processx")
  to_install <- setdiff(required_pkgs, rownames(installed.packages()))
  if (length(to_install) > 0) install.packages(to_install, repos = "https://cran.r-project.org")
  lapply(required_pkgs, library, character.only = TRUE)
  if (system("xidel --version", ignore.stdout = TRUE, ignore.stderr = TRUE) != 0) {
    stop("xidel not installed. Install it and restart R.")
  }
  options(timeout = 240)
  invisible(collector)
}

#' Validate JSON file.
#' @param collector Collector object.
#' @param file_name JSON file path.
#' @return Logical: valid or not.
validate_json.Collector <- function(collector, file_name) {
  json_data <- tryCatch(jsonlite::fromJSON(file_name), error = function(e) NULL)
  !is.null(json_data) && !is.null(json_data$result$hits$hit)
}

#' Get event URLs for DBLP.
#' @param collector Collector object.
#' @param event Event code.
#' @param year Year.
#' @return Vector of URLs.
get_event_urls.Collector <- function(collector, event, year) {
  if (event == "asiaccs" && year == 2019) {
    return(character(0))
  }
  if (event == "asiaccs") {
    c(
      sprintf("https://dblp.org/db/conf/asiaccs/asiaccs%d.html", year),
      sprintf("https://dblp.org/db/conf/ccs/asiaccs%d.html", year)
    )
  } else if (event == "sacmat") {
    sprintf("https://dblp.org/db/conf/sacmat/sacmat%d.html", year)
  } else {
    sprintf("https://dblp.org/db/conf/%s/%s%d.html", event, event, year)
  }
}

#' Download JSON files from DBLP for events and years.
#' @param collector A Collector object.
#' @return The modified Collector object (invisibly).
download_json.Collector <- function(collector) {
  stopifnot(inherits(collector, "Collector"))
  if (!file.exists(collector$log_file)) {
    write.csv(
      data.frame(
        Event = character(),
        Year = integer(),
        File = character(),
        URL = character(),
        HTTP_Code = character(),
        Status = character(),
        Message = character(),
        Timestamp = character(),
        stringsAsFactors = FALSE
      ),
      collector$log_file,
      row.names = FALSE,
      fileEncoding = "UTF-8"
    )
  }
  collector$log_df <- read.csv(collector$log_file, stringsAsFactors = FALSE, fileEncoding = "UTF-8")
  required_log_columns <- c("Event", "Year", "File", "URL", "HTTP_Code", "Status", "Message", "Timestamp")
  missing_columns <- setdiff(required_log_columns, colnames(collector$log_df))
  for (col in missing_columns) {
    collector$log_df[[col]] <- NA
  }
  collector$log_df <- collector$log_df[, required_log_columns, drop = FALSE]
  total_events <- length(collector$events) * length(collector$years)
  pb <- txtProgressBar(min = 0, max = total_events, style = 3)
  event_count <- 0
  max_attempts <- 3
  for (event in collector$events) {
    for (year in collector$years) {
      event_count <- event_count + 1
      setTxtProgressBar(pb, event_count)
      file_name <- file.path(collector$json_dir, sprintf("data_%s%d.json", event, year))
      status <- "fail"
      msg <- ""
      page_url <- ""
      code <- NA
      timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      if (file.exists(file_name) && validate_json.Collector(collector, file_name)) {
        cat(sprintf("\n✓ Valid file exists: %s\n", file_name))
        status <- "valid"
      } else {
        urls <- get_event_urls.Collector(collector, event, year)
        if (length(urls) == 0) {
          status <- "skipped"
          msg <- sprintf("Skipped %s %d due to configuration", event, year)
          cat(sprintf("\nℹ %s\n", msg))
        } else {
          success <- FALSE
          for (page_url in urls) {
            for (attempt in seq_len(max_attempts)) {
              current_agent <- sample(collector$user_agents, 1)
              cat(sprintf("\n→ Downloading: %s (try %d/%d) with User-Agent: %s\n",
                          page_url, attempt, max_attempts, current_agent))
              resp <- tryCatch(
                httr::GET(
                  page_url,
                  httr::timeout(120),
                  httr::add_headers(.headers = c(
                    `User-Agent` = current_agent,
                    collector$headers,
                    Referer = page_url
                  ))
                ),
                error = function(e) {
                  msg <<- sprintf("Network error for %s %d: %s", event, year, e$message)
                  cat(sprintf("⚠ %s\n", msg))
                  NULL
                }
              )
              if (is.null(resp)) {
                msg <- sprintf("No response for %s %d at %s", event, year, page_url)
                cat(sprintf("⚠ %s (try %d)\n", msg, attempt))
                Sys.sleep(runif(1, 5, 15))
                next
              }
              code <- httr::status_code(resp)
              if (code == 200) {
                webpage <- tryCatch(
                  rvest::read_html(httr::content(resp, "text", encoding = "UTF-8")),
                  error = function(e) {
                    msg <<- sprintf("HTML parsing failed for %s %d: %s", event, year, e$message)
                    cat(sprintf("⚠ %s\n", msg))
                    NULL
                  }
                )
                if (is.null(webpage)) {
                  Sys.sleep(runif(1, 5, 15))
                  next
                }
                json_link <- rvest::html_nodes(webpage, xpath = "//a[contains(@href, 'format=json')]") |>
                  rvest::html_attr("href")
                if (length(json_link) == 0 || is.na(json_link[1])) {
                  msg <- sprintf("JSON link missing for %s %d at %s", event, year, page_url)
                  cat(sprintf("✗ %s\n", msg))
                  break
                }
                json_link <- json_link[1]
                if (!startsWith(json_link, "http")) {
                  json_link <- paste0("https://dblp.org", json_link)
                }
                current_agent <- sample(collector$user_agents, 1)
                json_resp <- tryCatch(
                  httr::GET(
                    json_link,
                    httr::timeout(120),
                    httr::add_headers(.headers = c(
                      `User-Agent` = current_agent,
                      collector$headers,
                      Referer = page_url
                    ))
                  ),
                  error = function(e) {
                    msg <<- sprintf("JSON fetch error for %s %d: %s", event, year, e$message)
                    cat(sprintf("⚠ %s\n", msg))
                    NULL
                  }
                )
                if (is.null(json_resp)) {
                  msg <- sprintf("No JSON response for %s %d at %s", event, year, json_link)
                  cat(sprintf("⚠ %s\n", msg))
                  Sys.sleep(runif(1, 5, 15))
                  next
                }
                code <- httr::status_code(json_resp)
                if (code == 200) {
                  json_data <- tryCatch(
                    jsonlite::fromJSON(httr::content(json_resp, "text", encoding = "UTF-8")),
                    error = function(e) {
                      msg <<- sprintf("JSON parsing failed for %s %d: %s", event, year, e$message)
                      cat(sprintf("✗ %s\n", msg))
                      NULL
                    }
                  )
                  if (!is.null(json_data)) {
                    jsonlite::write_json(json_data, file_name, auto_unbox = TRUE)
                    if (validate_json.Collector(collector, file_name)) {
                      cat(sprintf("✓ Saved and validated: %s\n", file_name))
                      status <- "downloaded"
                      success <- TRUE
                      Sys.sleep(runif(1, 2, 8))
                      break
                    } else {
                      msg <- sprintf("Corrupted JSON file: %s", file_name)
                      cat(sprintf("✗ %s\n", msg))
                      status <- "corrupt"
                      if (file.exists(file_name)) file.remove(file_name)
                    }
                  }
                } else if (code %in% c(429, 403)) {
                  status <- "blocked"
                  msg <- sprintf("HTTP %d for JSON fetch at %s", code, json_link)
                  cat(sprintf("⚠ %s\n", msg))
                  retry_after <- as.numeric(httr::headers(json_resp)$`retry-after` %||% 10)
                  Sys.sleep(retry_after)
                } else {
                  msg <- sprintf("Failed to fetch JSON for %s %d (HTTP %d)", event, year, code)
                  cat(sprintf("⚠ %s\n", msg))
                }
              } else if (code %in% c(429, 403)) {
                status <- "blocked"
                msg <- sprintf("HTTP %d for %s %d at %s", code, event, year, page_url)
                cat(sprintf("⚠ %s\n", msg))
                retry_after <- as.numeric(httr::headers(resp)$`retry-after` %||% 10)
                Sys.sleep(retry_after)
              } else {
                msg <- sprintf("HTTP status %d for %s %d at %s", code, event, year, page_url)
                cat(sprintf("⚠ %s (try %d)\n", msg, attempt))
              }
              if (success) break
              Sys.sleep(runif(1, 5, 15))
            }
            if (success) break
          }
          if (!success && event == "asiaccs") {
            status <- "fail"
            msg <- sprintf("All attempts failed for %s %d", event, year)
            cat(sprintf("✗ %s\n", msg))
          }
        }
      }
      collector$log_df <- collector$log_df[!(collector$log_df$Event == event & collector$log_df$Year == year), , drop = FALSE]
      collector$log_df <- rbind(
        collector$log_df,
        data.frame(
          Event = event,
          Year = year,
          File = file_name,
          URL = page_url,
          HTTP_Code = code,
          Status = status,
          Message = msg,
          Timestamp = timestamp,
          stringsAsFactors = FALSE
        )
      )
      gc()
    }
  }
  close(pb)
  write.csv(collector$log_df, collector$log_file, row.names = FALSE, fileEncoding = "UTF-8")
  cat("ℹ JSON download complete\n")
  invisible(collector)
}
#' Consolidate JSON data into master dataset.
#' @param collector A Collector object.
#' @return The modified Collector object (invisibly).
consolidate_data.Collector <- function(collector) {
  stopifnot(inherits(collector, "Collector"))
  files <- list.files(collector$json_dir, pattern = "\\.json$", full.names = TRUE)
  cat(sprintf("ℹ %d JSON files found\n", length(files)))
  if (length(files) == 0) {
    cat("No JSON files found. Skipping consolidation.\n")
    return(invisible(collector))
  }
  df_list <- list()
  pb <- txtProgressBar(min = 0, max = length(files), style = 3)
  for (i in seq_along(files)) {
    setTxtProgressBar(pb, i)
    f <- files[i]
    if (!validate_json.Collector(collector, f)) {
      cat(sprintf("\n⚠ Skipping invalid JSON: %s\n", f))
      next
    }
    json_data <- tryCatch({
      jsonlite::fromJSON(f)
    }, error = function(e) {
      cat(sprintf("\n⚠ JSON parsing failed for %s: %s\n", f, e$message))
      NULL
    })
    if (is.null(json_data)) next
    hits <- json_data$result$hits$hit
    if (is.null(hits)) next
    for (j in seq_len(nrow(hits))) {
      hit <- hits[j, ]
      info <- hit$info
      authors <- NA_character_
      if (!is.null(info$authors) && !is.null(info$authors$author)) {
        authors <- info$authors$author
        if (is.list(authors)) {
          authors <- sapply(authors, function(x) {
            if (is.list(x$text)) paste(unlist(x$text), collapse = ", ") else as.character(x$text)
          })
        } else {
          authors <- as.character(authors$text)
        }
        authors <- paste(authors, collapse = ", ")
      }
      df <- data.frame(
        Score = hit$`@score` %||% NA,
        ID = hit$`@id` %||% NA,
        Authors = authors,
        Title = info$title %||% NA,
        Venue = info$venue %||% NA,
        Pages = info$pages %||% NA,
        Year = info$year %||% NA,
        Type = info$type %||% NA,
        Access = info$access %||% NA,
        Key = info$key %||% NA,
        EE = info$ee %||% NA,
        URL = info$url %||% NA,
        stringsAsFactors = FALSE
      )
      if (is.na(df$Title) || is.na(df$Year) || is.na(df$ID)) {
        cat(sprintf("\n⚠ Skipping record with missing title, year, or ID: %s\n", f))
        next
      }
      df_list[[length(df_list) + 1]] <- df
    }
  }
  close(pb)
  if (length(df_list) == 0) stop("No valid data loaded.")
  dataset <- do.call(rbind, df_list)
  dataset <- subset(dataset, tolower(Type) != "editorship")
  cat(sprintf("\nℹ Consolidated %d entries\n", nrow(dataset)))
  normalize_event <- function(venue) {
    v <- tolower(trimws(venue))
    v <- gsub("&", "&", v, fixed = TRUE)
    if (v %in% c("ccs", "acm ccs")) return("ACM CCS")
    if (grepl("asiaccs|asia[ -]?ccs", v)) return("ACM ASIA CCS")
    if (grepl("euro", v)) return("IEEE EURO S&P")
    if (grepl("ndss", v)) return("NDSS")
    if (grepl("usenix", v)) return("USENIX Security")
    if (grepl("sp$", v) || grepl("symposium on security and privacy", v)) return("IEEE S&P")
    if (grepl("hotnets", v)) return("HotNets")
    if (grepl("sacmat", v)) return("ACM SACMAT")
    return(venue)
  }
  dataset$Event <- vapply(dataset$Venue, normalize_event, character(1))
  dataset$Abstract <- NA_character_
  if (file.exists(collector$master_file)) {
    collector$master_df <- readRDS(collector$master_file)
    new_entries <- subset(dataset, !(ID %in% collector$master_df$ID))
    if (nrow(new_entries) > 0) {
      collector$master_df <- rbind(collector$master_df, new_entries)
      collector$master_df <- collector$master_df[!duplicated(collector$master_df$ID), ]
      saveRDS(collector$master_df, collector$master_file)
      write.csv(collector$master_df, collector$csv_file, row.names = FALSE, fileEncoding = "UTF-8")
      cat(sprintf("✓ Added %d new articles\n", nrow(new_entries)))
    }
  } else {
    collector$master_df <- dataset
    saveRDS(collector$master_df, collector$master_file)
    write.csv(collector$master_df, collector$csv_file, row.names = FALSE, fileEncoding = "UTF-8")
    cat("✓ Master dataset created\n")
  }
  if (interactive() && !is.null(collector$master_df) && nrow(collector$master_df) > 0) {
    cat("ℹ Displaying initial master dataset.\n")
    View(collector$master_df, title = "Initial Master Dataset")
  } else {
    cat("ℹ Master dataset is empty or not available, skipping display.\n")
  }
  cat("ℹ Data consolidation complete\n")
  invisible(collector)
}
#' Generate Xidel command for abstract extraction.
#' @param url URL of the paper.
#' @param user_agents List of user agents.
#' @return Xidel command string or NA.
xidel_cmd <- function(url, user_agents) {
  if (is.na(url) || url == "") return(NA)
  current_agent <- sample(user_agents, 1)
  xidel_base <- sprintf('xidel "%s" --user-agent="%s" --header "Accept: text/html,application/xhtml+xml,application/xml;q=0.9" -se ', url, current_agent)
  cmd_map <- list(
    acm = list(
      pattern = "^https://dl.acm.org/doi/|^https://doi.org/10\\.1145/",
      cmd = 'normalize-space(string(//*[@id="abstract"]))'
    ),
    ieee = list(
      pattern = "^https://ieeexplore.ieee.org/|^https://doi.org/10\\.1109/",
      cmd = 'string(//script[contains(.,"xplGlobal.document.metadata")]) | grep -Po \\\'"abstract"\\\\s*:\\\\s*"\\\\K([^"]+)\\\''
    ),
    usenix = list(
      pattern = "usenix",
      cmd = 'normalize-space(((//main//section)[1]//p)[2])'
    ),
    ndss = list(
      pattern = "ndss-symposium",
      cmd = 'normalize-space(string-join((//div[@class="paper-data"]//p)[position() > 1], " "))'
    ),
    sacmat = list(
      pattern = "^https://dl.acm.org/doi/|^https://doi.org/10\\.1145/",
      cmd = 'normalize-space(string(//*[@id="abstract"]))'
    )
  )
  for (entry in cmd_map) {
    if (grepl(entry$pattern, url, ignore.case = TRUE)) {
      return(sprintf("%s'%s'", xidel_base, entry$cmd))
    }
  }
  return(NA)
}
#' Fetch abstract from Semantic Scholar API.
#' @param doi DOI of the paper.
#' @param collector Collector object.
#' @return Abstract or NA.
get_semanticscholar_abstract <- function(doi, collector) {
  if (is.na(doi) || !grepl("^10\\.", doi)) return(NA)
  url <- paste0("https://api.semanticscholar.org/graph/v1/paper/DOI:", doi, "?fields=abstract")
  res <- tryCatch({
    httr::GET(
      url,
      httr::add_headers(.headers = collector$headers, `User-Agent` = sample(collector$user_agents, 1)),
      httr::timeout(120)
    )
  }, error = function(e) {
    cat(sprintf("⚠ Semantic Scholar fetch failed for DOI %s: %s\n", doi, e$message))
    NULL
  })
  if (is.null(res) || httr::status_code(res) != 200) return(NA)
  data <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  abs <- data$abstract
  if (is.null(abs) || nchar(abs) < 100) return(NA)
  Sys.sleep(runif(1, collector$request_interval, collector$request_interval * 3))
  trimws(abs)
}
#' Fetch abstract from OpenAlex API.
#' @param doi DOI of the paper.
#' @param collector Collector object.
#' @return Abstract or NA.
get_openalex_abstract <- function(doi, collector) {
  if (is.na(doi) || !grepl("^10\\.", doi)) return(NA)
  url <- paste0("https://api.openalex.org/works/https://doi.org/", URLencode(doi))
  res <- tryCatch({
    httr::GET(
      url,
      httr::add_headers(.headers = collector$headers, `User-Agent` = sample(collector$user_agents, 1)),
      httr::timeout(120)
    )
  }, error = function(e) {
    cat(sprintf("⚠ OpenAlex fetch failed for DOI %s: %s\n", doi, e$message))
    NULL
  })
  if (is.null(res) || httr::status_code(res) != 200) return(NA)
  data <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  inv_idx <- data$abstract_inverted_index
  if (is.null(inv_idx)) return(NA)
  max_pos <- max(unlist(inv_idx))
  words <- character(max_pos + 1)
  for (w in names(inv_idx)) {
    positions <- inv_idx[[w]] + 1
    words[positions] <- w
  }
  abs <- paste(words, collapse = " ")
  if (nchar(abs) < 100) return(NA)
  Sys.sleep(runif(1, collector$request_interval, collector$request_interval * 3))
  trimws(abs)
}
#' Fetch abstract from CrossRef API.
#' @param doi DOI of the paper.
#' @param collector Collector object.
#' @return Abstract or NA.
get_crossref_abstract <- function(doi, collector) {
  if (is.na(doi) || !grepl("^10\\.", doi)) return(NA)
  url <- paste0("https://api.crossref.org/works/", doi)
  res <- tryCatch({
    httr::GET(
      url,
      httr::add_headers(.headers = collector$headers, `User-Agent` = sample(collector$user_agents, 1)),
      httr::timeout(120)
    )
  }, error = function(e) {
    cat(sprintf("⚠ CrossRef fetch failed for DOI %s: %s\n", doi, e$message))
    NULL
  })
  if (is.null(res) || httr::status_code(res) != 200) return(NA)
  data <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  abs <- data$message$abstract
  if (is.null(abs)) return(NA)
  abs_clean <- gsub("<.*?>", "", abs)
  if (nchar(abs_clean) < 100) return(NA)
  Sys.sleep(runif(1, collector$request_interval, collector$request_interval * 3))
  trimws(abs_clean)
}
#' Extract abstract from URL or fallbacks.
#' @param ee_url Primary URL.
#' @param doi DOI.
#' @param collector Collector object.
#' @return List with abstract, status, message, source.
extract_abstract <- function(ee_url, doi, collector) {
  abs <- NA_character_
  status <- "fail"
  msg <- ""
  source_used <- ""
  if (grepl("proceedings", collector$master_df$Type[collector$master_df$ID == doi], ignore.case = TRUE)) {
    return(list(abs = "Not applicable", status = "n.a.", msg = "Proceedings; no abstract", source = "none"))
  }
  event <- collector$master_df$Event[collector$master_df$ID == doi]
  extractor_class <- switch(
    tolower(event),
    "acm ccs" = "ACMExtractor",
    "acm asia ccs" = "ACMExtractor",
    "acm sacmat" = "ACMExtractor",
    "hotnets" = "ACMExtractor",
    "usenix security" = "USENIXExtractor",
    "ndss" = "NDSSExtractor",
    "ieee s&p" = "IEEEExtractor",
    "ieee euro s&p" = "IEEEExtractor",
    "ACMExtractor" # Default
  )
  if (grepl("^https://dl.acm.org/doi/|^https://doi.org/10\\.1145/", ee_url)) {
    failure_count <- collector$acm_failure_counts[[ee_url]] %||% 0
    if (failure_count >= collector$acm_failure_threshold) {
      msg <- sprintf("Skipping ACM URL %s due to %d consecutive 403 errors", ee_url, failure_count)
      cat(sprintf("⚠ %s\n", msg))
      return(list(abs = NA_character_, status = "fail", msg = msg, source = "none"))
    }
    if (Sys.time() < collector$acm_blocked_until) {
      wait_time <- as.numeric(collector$acm_blocked_until - Sys.time(), units = "secs")
      if (wait_time > 0) {
        cat(sprintf("ℹ Waiting %0.1f s for ACM rate limit\n", wait_time))
        Sys.sleep(wait_time)
      }
    }
  }
  extractor <- do.call(extractor_class, list())
  abs <- extract(extractor, ee_url, doi, collector)
  if (!is.na(abs)) {
    status <- "ok"
    source_used <- class(extractor)[1]
    msg <- sprintf("Abstract fetched via %s", source_used)
    cat(sprintf("✓ %s for %s (%s)\n", msg, ee_url, event))
    return(list(abs = abs, status = status, msg = msg, source = source_used))
  }
  for (fn in list(get_semanticscholar_abstract, get_openalex_abstract, get_crossref_abstract)) {
    abs <- fn(doi, collector)
    if (!is.na(abs)) {
      status <- "ok"
      source_used <- deparse(substitute(fn))
      msg <- sprintf("Fallback abstract fetched via %s", source_used)
      cat(sprintf("ℹ %s for DOI %s (%s)\n", msg, doi, event))
      return(list(abs = abs, status = status, msg = msg, source = source_used))
    }
  }
  msg <- sprintf("No abstract or too short for DOI %s after all attempts", doi)
  cat(sprintf("⚠ %s (%s)\n", msg, event))
  list(abs = NA_character_, status = "fail", msg = msg, source = "none")
}
#' Extract abstracts with interleaving.
#' @param collector Collector object.
#' @param show_progress Show progress (default FALSE).
#' @return Modified collector (invisibly).
extract_abstracts.Collector <- function(collector, show_progress = FALSE) {
  stopifnot(inherits(collector, "Collector"))
  if (!file.exists(collector$abstract_log)) {
    write.csv(
      data.frame(
        ID = character(),
        Event = character(),
        EE = character(),
        Status = character(),
        Abstract = character(),
        Message = character(),
        Source = character(),
        Timestamp = character(),
        stringsAsFactors = FALSE
      ),
      collector$abstract_log,
      row.names = FALSE,
      fileEncoding = "UTF-8"
    )
  }
  get_to_process <- function() {
    is_empty <- is.na(collector$master_df$Abstract) | collector$master_df$Abstract == ""
    subset(collector$master_df, !is.na(EE) & is_empty)
  }
  repeat {
    to_process <- get_to_process()
    if (nrow(to_process) == 0) break
    # Global shuffle to mix events across batches
    to_process <- to_process[sample(nrow(to_process)), ]
    to_process <- head(to_process, collector$batch_size)
    # Additional shuffle within batch
    to_process <- to_process[sample(nrow(to_process)), ]
    acm_rows <- to_process$Event %in% c("ACM CCS", "ACM ASIA CCS", "ACM SACMAT", "HotNets") |
      grepl("^https://dl.acm.org/doi/|^https://doi.org/10\\.1145/", to_process$EE)
    to_process$is_acm <- acm_rows
    if (any(!acm_rows)) {
      to_process_non_acm <- to_process[!acm_rows, ]
      to_process_acm <- to_process[acm_rows, ]
      to_process_non_acm <- to_process_non_acm[sample(nrow(to_process_non_acm)), ]
      to_process_acm <- to_process_acm[sample(nrow(to_process_acm)), ]
      to_process <- data.frame()
      i_acm <- 1
      i_non_acm <- 1
      while (i_acm <= nrow(to_process_acm) || i_non_acm <= nrow(to_process_non_acm)) {
        if (i_non_acm <= nrow(to_process_non_acm) && (!collector$last_was_acm || i_acm > nrow(to_process_acm))) {
          to_process <- rbind(to_process, to_process_non_acm[i_non_acm, ])
          i_non_acm <- i_non_acm + 1
        } else if (i_acm <= nrow(to_process_acm)) {
          to_process <- rbind(to_process, to_process_acm[i_acm, ])
          i_acm <- i_acm + 1
        }
      }
    } else {
      to_process <- to_process[sample(nrow(to_process)), ]
    }
    pb <- txtProgressBar(min = 0, max = nrow(to_process), style = 3)
    processed <- 0
    for (i in seq_len(nrow(to_process))) {
      processed <- processed + 1
      setTxtProgressBar(pb, processed)
      row <- to_process[i, ]
      ee_url <- as.character(row$EE)
      id <- as.character(row$ID)
      event <- as.character(row$Event)
      is_acm <- row$is_acm
      timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      idx <- which(collector$master_df$ID == id)
      if (length(idx) == 0) {
        cat(sprintf("⚠ ID %s not found in master_df, skipping update\n", id))
        next
      }
      result <- extract_abstract(ee_url, id, collector)
      abs <- result$abs
      status <- result$status
      msg <- result$msg
      source <- result$source
      if ((is.na(collector$master_df$Abstract[idx]) ||
           collector$master_df$Abstract[idx] == "") &&
          (status %in% c("ok", "n.a.")) &&
          (status == "n.a." || nchar(abs) >= 100)) {
        collector$master_df$Abstract[idx] <- abs
        cat(sprintf("✓ Updated Abstract for ID %s at index %d (%s)\n", id, idx, event))
      } else {
        cat(sprintf("ℹ No update for ID %s: status=%s, nchar(abs)=%d (%s)\n", id, status, nchar(abs), event))
      }
      log_entry <- data.frame(
        ID = id,
        Event = event,
        EE = ee_url,
        Status = status,
        Abstract = abs,
        Message = msg,
        Source = source,
        Timestamp = timestamp,
        stringsAsFactors = FALSE
      )
      write.table(
        log_entry,
        collector$abstract_log,
        append = TRUE,
        row.names = FALSE,
        col.names = !file.exists(collector$abstract_log),
        sep = ",",
        fileEncoding = "UTF-8"
      )
      if (status == "ok") {
        cat(sprintf("\n✓ Abstract fetched: %s (%s)\n", id, event))
        cat(sprintf(" Title: %s\n Authors: %s\n Abstract: %s\n",
                    collector$master_df$Title[idx],
                    collector$master_df$Authors[idx],
                    substr(abs, 1, 100)))
        collector$acm_backoff <- 60
      } else if (status == "n.a.") {
        cat(sprintf("\nℹ No abstract (proceedings): %s (%s)\n", id, event))
      } else {
        cat(sprintf("\n⚠ Failed to fetch abstract: %s (%s) (%s)\n", id, msg, event))
        if (grepl("403", msg)) {
          collector$acm_blocked_until <- Sys.time() + collector$acm_backoff
          collector$acm_backoff <- min(collector$acm_backoff * 2, collector$acm_max_backoff)
        }
      }
      if (status == "ok") {
        wait_time <- runif(1, collector$request_interval, collector$request_interval * 3)
        cat(sprintf("ℹ Abstract fetched successfully for %s. Waiting %0.1f seconds before next request\n", event, wait_time))
        Sys.sleep(wait_time)
        collector$last_was_acm <- is_acm
      } else if (is_acm && (source == "xidel" || (status == "fail" && grepl("^https://dl.acm.org/doi/|^https://doi.org/10\\.1145/", ee_url)))) {
        wait_time <- runif(1, collector$acm_wait_min, collector$acm_wait_max)
        cat(sprintf("ℹ ACM paper processed via xidel or failed for %s. Waiting %0.1f minutes before next request\n", event, wait_time / 60))
        Sys.sleep(wait_time)
        collector$last_was_acm <- TRUE
      } else {
        wait_time <- runif(1, collector$request_interval, collector$request_interval * 3)
        cat(sprintf("ℹ Non-ACM paper or fallback API used for %s. Waiting %0.1f seconds before next request\n", event, wait_time))
        Sys.sleep(wait_time)
        collector$last_was_acm <- FALSE
      }
    }
    close(pb)
    saveRDS(collector$master_df, collector$master_file)
    write.csv(collector$master_df, collector$csv_file, row.names = FALSE, fileEncoding = "UTF-8")
    cat(sprintf("✓ Master dataset updated with %d abstracts\n", sum(!is.na(collector$master_df$Abstract) & collector$master_df$Abstract != "")))
    wait_time <- runif(1, 60, 600)
    cat(sprintf("ℹ Batch complete. Waiting %0.1f minutes before next batch to avoid Cloudflare blocks\n", wait_time / 60))
    if (show_progress && interactive()) {
      View(collector$master_df, title = "Updated Master Dataset")
    }
    Sys.sleep(wait_time)
  }
  if (interactive() && !is.null(collector$master_df) && nrow(collector$master_df) > 0) {
    cat("ℹ Displaying final master dataset.\n")
    View(collector$master_df, title = "Final Master Dataset")
  } else {
    cat("ℹ Master dataset is empty or not available, skipping display.\n")
  }
  cat("ℹ Abstract extraction complete\n")
  invisible(collector)
}
#' Search master dataset for keywords.
#' @param keyword Search term.
#' @param columns Columns to search (default Title, Abstract).
#' @param event Optional event filter.
#' @param year Optional year filter.
#' @param master_file Path to master dataset.
#' @return Data frame of results or NULL.
search_master_df <- function(keyword, columns = c("Title", "Abstract"), event = NULL, year = NULL, master_file = "top4/data/master_dataset.rds") {
  stopifnot(is.character(keyword), is.character(columns), file.exists(master_file))
  master_df <- readRDS(master_file)
  results <- data.frame()
  for (col in columns) {
    if (!(col %in% colnames(master_df))) {
      cat(sprintf("⚠ Column %s not found in master dataset\n", col))
      next
    }
    matches <- master_df[grep(keyword, master_df[[col]], ignore.case = TRUE), ]
    if (!is.null(event)) matches <- matches[matches$Event == event, ]
    if (!is.null(year)) matches <- matches[matches$Year == year, ]
    if (nrow(matches) > 0) {
      matches$SearchColumn <- col
      results <- rbind(results, matches)
    }
  }
  if (nrow(results) == 0) {
    cat(sprintf("ℹ No matches found for '%s' in columns: %s\n", keyword, paste(columns, collapse = ", ")))
    return(NULL)
  }
  results <- results[!duplicated(results$ID), c("ID", "Title", "Authors", "Event", "Year", "Abstract", "SearchColumn")]
  if (interactive()) {
    cat(sprintf("ℹ Found %d matches for '%s'\n", nrow(results), keyword))
    View(results, title = sprintf("Search Results: %s", keyword))
  }
  results
}
#' Main execution function.
#' @param monitor Run in monitoring mode (default FALSE).
#' @param monitor_interval Interval for monitoring (default 3600 seconds).
#' @return Collector object (invisibly).
main <- function(monitor = FALSE, monitor_interval = 3600) {
  collector <- Collector()
  collector <- setup.Collector(collector)
  repeat {
    collector <- download_json.Collector(collector)
    collector <- consolidate_data.Collector(collector)
    collector <- extract_abstracts.Collector(collector, show_progress = TRUE)
    if (!monitor) break
    cat(sprintf("ℹ Monitoring mode: Waiting %d seconds before next cycle\n", monitor_interval))
    Sys.sleep(monitor_interval)
  }
  invisible(collector)
}
# Run the script
if (!interactive()) {
  main(monitor = FALSE)
} else {
  main(monitor = FALSE)
}