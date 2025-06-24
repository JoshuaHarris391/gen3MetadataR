#' Gen3 Metadata Parser
#'
#' A class to interact with Gen3 metadata API for fetching and processing data.
#' Perfect for cancer genomics research workflows.
#'
#' @field key_file_path Path to the JSON authentication key file
#' @field headers HTTP headers for API authentication  
#' @field data_store Storage for raw JSON data
#' @field data_store_pd Storage for processed data frames
#'
#' @importFrom R6 R6Class
#' @importFrom jsonlite fromJSON
#' @importFrom httr2 request req_method req_body_json req_perform resp_body_json resp_status req_headers
#' @importFrom stringr str_detect str_replace_all str_remove
#' @importFrom base64enc base64decode
#' @importFrom magrittr %>%
#'
#' @examples
#' \dontrun{
#' parser <- Gen3MetadataParser$new("path/to/keyfile.json")
#' parser$authenticate()
#' parser$fetch_data("my_program", "my_project", "sample")
#' }
#'
#' @export

Gen3MetadataParser <- R6Class("Gen3MetadataParser",
  public = list(
    # Instance variables (like Python's __init__)
    key_file_path = NULL,
    headers = NULL,
    data_store = NULL,
    data_store_pd = NULL,
    
    #' @description
    #' Initialize a new Gen3MetadataParser object.
    #' Sets up storage and headers, and stores the path to the key file.
    #' 
    #' @param key_file_path Path to the JSON authentication key file.
    initialize = function(key_file_path) {
      self$key_file_path <- key_file_path
      self$headers <- list()
      self$data_store <- list()
      self$data_store_pd <- list()
    },
    
    #' @description
    #' Attempt to fix malformed JSON by adding quotes around keys and string values.
    #' Used internally when loading API key files that may not be properly formatted.
    #'
    #' @param input_str A string containing the JSON to be fixed.
    add_quotes_to_json = function(input_str) {
      tryCatch({
        # Try parsing as-is
        return(fromJSON(input_str))
      }, error = function(e) {
        tryCatch({
          # Add quotes around keys
          fixed <- str_replace_all(input_str, "([{,]\\s*)(\\w+)\\s*:", "\\1\"\\2\":")
          # Add quotes around simple string values
          fixed <- str_replace_all(fixed, ":\\s*([A-Za-z0-9._:@/-]+)(?=\\s*[},])", ": \"\\1\"")
          return(fromJSON(fixed))
        }, error = function(e2) {
          stop(paste("Could not fix JSON:", e2$message))
        })
      })
    },
    
    #' @description
    #' Load the API key from a JSON file.
    #' Handles both well-formed and some malformed JSON files.
    #' Returns a list representing the credentials.
    #'
    #' @return A list representing the credentials.
    load_api_key = function() {
      tryCatch({
        # Read file as text first
        content <- readLines(self$key_file_path, warn = FALSE)
        content <- paste(content, collapse = "")
        
        # Check if content needs quote fixing
        if (!str_detect(content, '"') && !str_detect(content, "'")) {
          return(self$add_quotes_to_json(content))
        }
        
        # Try normal JSON parsing
        return(fromJSON(self$key_file_path))
        
      }, error = function(e) {
        if (str_detect(e$message, "cannot open the connection")) {
          stop(paste("File not found:", self$key_file_path))
        } else if (str_detect(e$message, "invalid")) {
          cat("JSON decode error:", e$message, "\n")
          cat("Please make sure the file contains valid JSON with quotes and proper formatting.\n")
          stop(e)
        } else {
          cat("An unexpected error occurred while loading API key:", e$message, "\n")
          stop(e)
        }
      })
    },
    
    #' @description
    #' Extract the base API URL from a JWT token found in the credentials.
    #' Decodes the JWT, extracts the issuer, and removes any '/user' suffix.
    #' Returns the base URL as a string.
    #'
    #' @param cred A list of credentials containing the JWT token.
    #' @return The base API URL as a string.
    url_from_jwt = function(cred) {
      jwt_token <- cred$api_key
      
      # JWT tokens have 3 parts separated by dots: header.payload.signature
      # We only need the payload (middle part)
      parts <- strsplit(jwt_token, "\\.")[[1]]
      
      if (length(parts) != 3) {
          stop("Invalid JWT token format")
      }
      
      # Decode the payload (base64url decoding)
      payload_encoded <- parts[2]
      
      # Add padding if needed (base64url doesn't always have padding)
      missing_padding <- 4 - (nchar(payload_encoded) %% 4)
      if (missing_padding != 4) {
          payload_encoded <- paste0(payload_encoded, paste(rep("=", missing_padding), collapse = ""))
      }
      
      # Convert base64url to base64 (replace - with + and _ with /)
      payload_base64 <- chartr("-_", "+/", payload_encoded)
      
      # Decode and parse JSON
      payload_json <- rawToChar(base64enc::base64decode(payload_base64))
      decoded <- fromJSON(payload_json)
      
      # Extract issuer and remove "/user" suffix
      url <- str_remove(decoded$iss %||% "", "/user$")
      return(url)
    },
    
    #' @description
    #' Authenticate with the Gen3 API using the loaded credentials.
    #' Obtains an access token and stores it in the headers for future requests.
    authenticate = function() {
      tryCatch({
        key <- self$load_api_key()
        api_url <- self$url_from_jwt(key)
        
        response <- request(paste0(api_url, "/user/credentials/cdis/access_token")) %>%
          req_method("POST") %>%
          req_body_json(key) %>%
          req_perform()
        
        access_token <- resp_body_json(response)$access_token
        self$headers <- list(Authorization = paste("bearer", access_token))
        
        cat("Authentication successful:", resp_status(response), "\n")
        
      }, error = function(e) {
        if (inherits(e, "httr2_http_error")) {
          cat("HTTP error occurred during authentication:", e$message, "\n")
          cat("Status Code:", resp_status(e$response), "\n")
        } else {
          cat("An unexpected error occurred during authentication:", e$message, "\n")
        }
        stop(e)
      })
    },
    
    #' @description
    #' Convert JSON data to a flattened data.frame.
    #' This is similar to converting nested JSON to a pandas DataFrame in Python.
    #' Returns a data.frame.
    #'
    #' @param json_data The JSON data to be converted to a data.frame.
    #' @return A flattened data.frame.
    json_to_pd = function(json_data) {
      # This is like flattening nested JSON into a nice tabular format
      # Use jsonlite::flatten to flatten nested data.frames
      df <- as.data.frame(json_data)
      if ("jsonlite" %in% rownames(installed.packages())) {
        df <- jsonlite::flatten(df)
      }
      return(df)
    },
    
    #' @description
    #' Fetch metadata from the Gen3 API for a given program, project, and node label.
    #' Stores the result in the data_store slot, and optionally returns the data.
    #' 
    #' @param program_name Name of the Gen3 program
    #' @param project_code Code of the Gen3 project
    #' @param node_label Node label to fetch (e.g., "sample")
    #' @param return_data If TRUE, returns the data instead of just storing it
    #' @param api_version API version string (default "v0")
    fetch_data = function(program_name, project_code, node_label, 
                         return_data = FALSE, api_version = "v0") {
      tryCatch({
        creds <- self$load_api_key()
        api_url <- self$url_from_jwt(creds)
        
        url <- paste0(api_url, "/api/", api_version, "/submission/", 
                     program_name, "/", project_code, 
                     "/export/?node_label=", node_label, "&format=json")
        
        response <- request(url) %>%
          req_headers(!!!self$headers) %>%
          req_perform()
        
        cat("status code:", resp_status(response), "\n")
        
        data <- resp_body_json(response)
        
        key <- paste(program_name, project_code, node_label, sep = "/")
        self$data_store[[key]] <- data
        
        if (return_data) {
          return(data)
        } else {
          cat("Data for", key, "has been fetched and stored.\n")
        }
        
      }, error = function(e) {
        if (inherits(e, "httr2_http_error")) {
          cat("HTTP error occurred:", e$message, "\n")
          cat("Status Code:", resp_status(e$response), "\n")
        } else {
          cat("An error occurred:", e$message, "\n")
        }
        stop(e)
      })
    },
    
    #' @description
    #' Convert all stored JSON metadata in data_store to data.frames and store in data_store_pd.
    #' Each key in data_store_pd corresponds to a key in data_store.
    data_to_pd = function() {
      for (key in names(self$data_store)) {
        cat("Converting", key, "to data.frame...\n")
        self$data_store_pd[[key]] <- self$json_to_pd(self$data_store[[key]]$data)
      }
      invisible(self)  # R equivalent of Python's None return
    }
  )
)
