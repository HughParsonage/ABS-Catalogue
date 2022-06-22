library(readabs)
library(magrittr)
library(data.table)
library(hutilscpp)
library(hutils)

if (TRUE || !exists("WPI") || !exists("CPI") || !exists("LFI")) {
  WPI_orig <- copy(WPI <- read_abs("6345.0", check_local = FALSE))
  CPI_orig <- copy(CPI <- read_abs("6401.0", check_local = FALSE))
  LFI_orig <- copy(LFI <- read_abs("6202.0", check_local = FALSE))
}

setDT(WPI)
setDT(CPI)
setDT(LFI)

# Some series are duplicated, we only want one data file per series
# The metadata will be slightly different -- have to cope with the first
LFI <- rbindlist(list(LFI, CPI, WPI), use.names = TRUE)
setkey(LFI, series_id, date)
LFI <- unique(LFI, by = c("series_id", "date"))



# No 000 or 000 Hours
LFI[unit == "000", c("unit", "value") := list("1", value * 1000)]
LFI[unit == "000 Hours", c("unit", "value") := list("hours", value * 1000)]

# Normalize tables
## Verify variables are constant per series_id
verify_columns_constant <- function(DT, 
                                    by_col = c("table_no", "series_id"),
                                    columns = c("table_no",
                                                "sheet_no",
                                                "table_title",
                                                "series", 
                                                # "value", 
                                                "series_type", "data_type", 
                                                "collection_month",
                                                "frequency", 
                                                "unit")) {
  if (!all(columns %chin% names(DT))) {
    stop("Some columns not present in names: ", setdiff(columns, names(DT)), "\n\n",
         "names(DT) = ", toString(names(DT)))
  }
  res <- DT[, lapply(.SD, isntConstant), by = c(by_col), .SDcols = c(columns)]
  names(res)[sapply(res, function(x) max(x, na.rm = TRUE) == 0L)]
}

lfi_constant_cols <- verify_columns_constant(LFI)

minmax_date_by_series_id <-
  LFI[, .(minDate = min(date), maxDate = max(date)), keyby = .(series_id)]

metadata_of_LFI <- 
  LFI %>%
  .[, lapply(.SD, first),
    .SDcols = c(lfi_constant_cols), 
    by = c("series_id")] %>%
  merge(minmax_date_by_series_id, 
        by = c("series_id"))



# new_lfi_cols <- names(metadata_of_LFI)[order(sapply(metadata_of_LFI, function(x) max(nchar(x), na.rm = TRUE)))]
new_lfi_cols <- 
  c("minDate", "maxDate",
    "collection_month", "sheet_no", "data_type", "frequency", "unit", 
    "table_no", "series_id", "series_type", "series", "table_title")
setcolorder(metadata_of_LFI, new_lfi_cols)
set_cols_first(metadata_of_LFI, c("table_no", "series_id"))

metadata_on_disk <- data.table()
if (file.exists("metadata.tsv")) {
  metadata_on_disk <- fread("metadata.tsv", sep = "\t")
  for (j in seq_along(metadata_on_disk)) {
    if (inherits(v <- .subset2(metadata_on_disk, j), "IDate")) {
      set(metadata_on_disk, j = j, value = as.Date(v))
    }
  }
}
fwrite(unique(rbind(metadata_of_LFI, metadata_on_disk, use.names = TRUE)),
       "metadata.tsv", 
       sep = "\t")


LFI[, 
    fwrite(if (.BY[["unit"]] == "1") {
      copy(.SD)[, value := as.integer(value)]  
    } else {
      .SD
    }, 
    file = provide.file(paste0("./data/series_id/", 
                               # for git tree performance
                               substr(.BY[["series_id"]], 1, 4),
                               "/",
                               .BY[["series_id"]], ".tsv")),
    sep = "\t"),
    keyby = .(series_id, unit),
    .SDcols = c("date", "value")]





                                    
