####################################
  # Bumped Inc.  // proprietary //
  # 2019-10-25
  # Andrew Pfaendler
  # andrew.pfaendler@bumped.com

  # R version 3.6.1 (2019-07-05)
  # R-Studio: Version 1.2.1335
####################################

# ltv pre versus post
library(ggplot2)
library(gridExtra)
library(psych)
library(reshape2)

dat <- read.csv("~/Desktop/R.data/ltv/pre_post_ltv.csv", header = TRUE)
f_dat <- melt(dat[c("pre_ltv", "post_ltv")])

describe(dat, IQR = TRUE)
describe(log(dat[c("pre_ltv", "post_ltv")]), IQR = TRUE)


# facet log histograms
ggplot(f_dat, aes(x = log(f_dat$value), y=..density..)) +
  geom_histogram(color = "grey30", fill = "white") +
  geom_density(alpha=0.2, fill="#FF6666") +
  facet_grid(f_dat$variable ~ .)+
  xlab("log LTV")+
  ggtitle("Log LTV pre/post Histograms")

# ratio change series histogram
ggplot(dat, aes(x = dat$rat, y=..density..)) +
  geom_histogram(color = "grey30", fill = "white") +
  geom_density(alpha=0.2, fill="#FF6666")+
  geom_vline(aes(xintercept=mean(dat$rat)), color="#20639B", linetype="dashed", size=1, alpha=0.3)+
  xlim(-1, 6)+
  xlab("% LTV Change")+
  ggtitle("LTV % Change Distribution")

# boxplots of log data
ggplot(data=f_dat, aes(x="", y=log(f_dat$value)))+
  geom_boxplot(notch = TRUE)+
  facet_grid(cols = vars(f_dat$variable))+
  ylab("Log LTV")+
  ggtitle("Log LTV BoxPlots")+
  theme(axis.title.x = element_blank())


# not normal
ks.test(x = log(dat$pre_ltv), y='pnorm', alternative='two.sided')
ks.test(x = log(dat$post_ltv), y='pnorm', alternative='two.sided')

# median diff_ltv test
wilcox.test(
  y = dat$pre_ltv,
  x = dat$post_ltv,
  alternative = "greater",
  paired = FALSE,
  conf.int = TRUE)
