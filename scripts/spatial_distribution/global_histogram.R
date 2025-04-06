library(sf)
library(MASS)
library(ggplot2)
library(dplyr)

dat <- st_read("../../results/hex_11_land.gpkg")

lower_bound <- quantile(dat$ratio, 0.01, na.rm = TRUE)  # 1st percentile
upper_bound <- quantile(dat$ratio, 0.99, na.rm = TRUE)  # 99th percentile

# Filter data
dat_filtered <- dat %>% filter(ratio >= lower_bound, ratio <= upper_bound)

params <- list(mean= d1$estimate[1], sd = d1$estimate[2])

ggplot(dat_filtered, aes(x=ratio)) + 
  geom_histogram(aes(y = after_stat(density)), binwidth = 0.01, fill = "lightblue") +
  stat_function(fun = dlnorm, args = params)