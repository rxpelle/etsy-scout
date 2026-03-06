# Etsy Scout

Free, open-source CLI tool for Etsy keyword research and competitor analysis. Built for sellers who want real data without paying for it.

Most Etsy keyword tools (eRank, Marmalead) charge monthly fees for data you can get yourself. Etsy Scout does it from your terminal — no subscriptions, no accounts, no servers.

## What It Does

**Mine keywords from autocomplete.** Give it seed phrases and it expands them into long-tail variations using search suggestion data. Same data the paid tools use.

**Import your Etsy Ads data.** If you're running Etsy Ads, you've got search term reports sitting in your dashboard. Etsy Scout imports those CSVs and cross-references them with your keyword database. Now you know which keywords actually convert.

**Score everything.** Every keyword gets a composite score based on autocomplete position, competition (listing count), engagement (favorites), and real ads performance. High scores = worth targeting. Low scores = stop wasting money.

**Track competitors.** Add competitor listing IDs and track their price, favorites, reviews, ratings, and sales over time.

**Export for Etsy.** Generate optimized listing tags (13 tags, 20 chars max) or export keyword CSVs for your ad campaigns.

## Install

Python 3.9 or higher.

```bash
git clone https://github.com/rxpelle/etsy-scout.git
cd etsy-scout
pip install -e .
```

## Quick Start

```bash
# Initialize the database
etsy-scout config init

# Mine keywords for your niche
etsy-scout mine "custom mug"
etsy-scout mine "vintage jewelry" --depth 2

# Score keywords
etsy-scout score

# See your top keywords
etsy-scout report keywords

# Export optimized tags for a listing
etsy-scout export tags
```

## Commands

### Keyword Research

```bash
etsy-scout mine "seed phrase"              # Mine autocomplete suggestions
etsy-scout mine "seed phrase" --depth 2    # Deep expansion (a-z on each result)
etsy-scout score                           # Score all keywords
etsy-scout score --recalculate             # Rescore everything
```

### Etsy Ads Integration

```bash
etsy-scout import-ads search-terms.csv                  # Import Etsy Ads CSV
etsy-scout import-ads report.csv --listing "My Product"  # Filter by listing
```

Download your search term report from Etsy Ads dashboard → Stats → Search Terms → Download CSV.

### Competitor Tracking

```bash
etsy-scout track add 1234567890 --own --name "My Listing"  # Track your listing
etsy-scout track add 9876543210                             # Track a competitor
etsy-scout track list                                       # Show all tracked
etsy-scout track snapshot                                   # Refresh all data
```

### Reports

```bash
etsy-scout report keywords                        # Top keywords by score
etsy-scout report keywords --format csv > kw.csv  # Export as CSV
etsy-scout report keywords --format json           # Export as JSON
etsy-scout report competitors                      # Side-by-side comparison
etsy-scout report ads                              # Ads performance summary
etsy-scout report gaps                             # Impressions but no orders
```

### Export

```bash
etsy-scout export tags                     # Optimized Etsy tags (13 max, ≤20 chars)
etsy-scout export tags --min-score 50      # Only high-scoring keywords
etsy-scout export ads                      # Keywords CSV for Etsy Ads
```

### Reverse Listing Lookup

```bash
etsy-scout reverse 1234567890              # Find what keywords a listing ranks for
etsy-scout reverse 1234567890 --top 50     # Only check top 50 keywords
```

## Keyword Scoring

Each keyword gets a composite score (0–205) based on:

| Signal | Points | What It Means |
|---|---|---|
| Autocomplete position | up to 100 | Higher position = more people searching |
| Low competition | up to 30 | Fewer competing listings = easier to rank |
| High engagement | up to 25 | Top results have lots of favorites = real demand |
| Ads impressions | up to 20 | Your ads appeared for this term |
| Ads orders | up to 50 | People actually bought from this term |

## Configuration

Copy `.env.example` to `.env` to customize:

```bash
cp .env.example .env
```

| Setting | Default | Description |
|---|---|---|
| `DB_PATH` | `data/etsy_scout.db` | SQLite database location |
| `ETSY_API_KEY` | *(not set)* | Optional Etsy API key for enhanced data |
| `PROXY_URL` | *(not set)* | HTTP proxy for avoiding rate limits |
| `AUTOCOMPLETE_RATE_LIMIT` | `0.5s` | Delay between autocomplete queries |
| `LISTING_SCRAPE_RATE_LIMIT` | `2.0s` | Delay between listing page scrapes |

## Limitations

- **It's a CLI tool.** No web dashboard. If you've never opened a terminal, there's a learning curve.
- **Autocomplete data comes from search suggestions.** Etsy could change their endpoints at any time. The difference is you're not out $30/month when it happens.
- **Listing scraping is rate-limited.** Don't run it aggressively or Etsy will serve CAPTCHAs. The default rate limits are conservative.

## License

MIT — use it, modify it, share it. No strings.

## Related

- [KDP Scout](https://github.com/rxpelle/kdp-scout) — Same concept, built for Amazon KDP keyword research
