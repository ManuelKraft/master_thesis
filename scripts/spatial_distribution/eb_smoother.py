###Implementation of the Global Linear Empirical Bayes Smoother, a special Empirical Bayes Smoother.
###Given a Dataset with proportion values, consisting of a nominator and a denominator.
###This implementation shrinks the proportion to an overall mean.
###The smaller the denominator and the higher the spread of all proportions, the more dominant the shrinkage effect
###The final Dataset has less Variance.

import geopandas as gpd
import pandas as pd

def prior_mean(nominators: pd.Series, denominators: pd.Series):
    #Literature for used Formula: from Anselin 2006, P.48, Formula 89
    return sum(nominators) / sum(denominators)

def prior_variance(prior_mean: float, nominators: pd.Series, denominators: pd.Series):
    #Literature for used Formula: from Anselin 2006, P.48, Formula 90
    proportions = nominators / denominators
    nominator_sum = 0
    mean_denominator = 0
    for denominator, proportion in zip(denominators, proportions):
        nominator_sum += denominator * ((proportion - prior_mean)**2)
        mean_denominator += (denominator / len(denominators))
    return nominator_sum / sum(denominators) - prior_mean / mean_denominator

def prior_distribution_weights(prior_mean: float, prior_variance: float, denominators: pd.Series):
    #Literature for used Formula: from Anselin 2006, P.48, Formula 69
    weights = []
    for denominator in denominators:
        weight = prior_variance / (prior_variance + (prior_mean / denominator))
        weights.append(weight)
    return weights

def eb_estimates(weights: list[float], proportions, prior_mean):
    #Literature for used Formula: from Anselin 2006, P.48, Formula 68
    eb_estimates = []
    for weight, proportion in zip(weights, proportions):
        eb_estimate = weight * proportion + (1 - weight) * prior_mean
        eb_estimates.append(eb_estimate)
    return eb_estimates


if __name__ == "main":
    input_dataset = gpd.read_file(f"../results/hexagons_11.gpkg")
    ids = input_dataset[input_dataset.ratio != -999].id
    deletions_counts = input_dataset[input_dataset.ratio != -999].count_deletions
    recent_counts = input_dataset[input_dataset.ratio != -999].count_visible
    proportions = input_dataset[input_dataset.ratio != -999].ratio

    mean = prior_mean(nominators=deletions_counts, denominators=recent_counts)
    variance = prior_variance(prior_mean=mean, nominators=deletions_counts, denominators=recent_counts)
    weights = prior_distribution_weights(prior_mean=mean, prior_variance=variance, denominators=recent_counts)
    print(mean, variance)
    proportion_new = eb_estimates(weights=weights, proportions=proportions, prior_mean=mean)
    dataset_new = gpd.GeoDataFrame(data={"proportion_old": input_dataset.proportion, "proportion_new": proportion_new, "weight": weights}, geometry=input_dataset.geometry)
    dataset_new.to_file(f"../results/hexagons_11_2.gpkg")

