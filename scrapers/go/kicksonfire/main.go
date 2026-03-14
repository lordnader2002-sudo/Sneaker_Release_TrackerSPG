// scrapers/go/kicksonfire/main.go
//
// Go port of fetch_release_kicksonfire.py
//
// Fetches the KicksOnFire sneaker release calendar via plain HTTP (no browser)
// and outputs a JSON array of release records to stdout or a file.
//
// Usage:
//   go run . --days 35 --output data/fallback_kicksonfire.json
//
// Build a static binary:
//   go build -o kicksonfire-scraper .
//   ./kicksonfire-scraper --days 35 --output data/fallback_kicksonfire.json
//
// The output schema is identical to the Python scrapers so it can be fed
// directly into merge_and_compare.py as a --fallback argument.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	sourceURL  = "https://www.kicksonfire.com/sneaker-release-dates"
	sourceName = "kicksonfire-go"
	userAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

// Release mirrors the JSON schema produced by all Python scrapers.
type Release struct {
	ReleaseDate          string  `json:"releaseDate"`
	ShoeName             string  `json:"shoeName"`
	Brand                string  `json:"brand"`
	RetailPrice          int     `json:"retailPrice"`
	EstimatedMarketValue *int    `json:"estimatedMarketValue"`
	ImageURL             *string `json:"imageUrl"`
	SourcePrimary        string  `json:"sourcePrimary"`
	SourceSecondary      string  `json:"sourceSecondary"`
	SourceURL            string  `json:"sourceUrl"`
	ReleaseURL           string  `json:"releaseUrl"`
}

// anchorRE matches KicksOnFire's "Jan 15 Nike Air Jordan..." link text pattern.
var anchorRE = regexp.MustCompile(`(?i)^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s+(.+)$`)

// labeledPriceRE finds "Retail Price: $130" / "MSRP $180" etc.
var labeledPriceRE = regexp.MustCompile(`(?i)\b(?:retail\s*price|msrp|price)\b\s*[:\-]?\s*[$£]\s*(\d{2,4})(?:\.\d{2})?`)

// monthMap converts abbreviated month names to month numbers.
var monthMap = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

func fetchHTML(url string) (*goquery.Document, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	return goquery.NewDocumentFromReader(resp.Body)
}

func parseDate(month, day string, year int) (string, bool) {
	m, ok := monthMap[strings.ToLower(month)]
	if !ok {
		return "", false
	}
	d, err := strconv.Atoi(day)
	if err != nil || d < 1 || d > 31 {
		return "", false
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, m, d), true
}

func extractLabeledPrice(text string) int {
	m := labeledPriceRE.FindStringSubmatch(text)
	if m == nil {
		return 0
	}
	v, err := strconv.Atoi(m[1])
	if err != nil || v < 40 || v > 700 {
		return 0
	}
	return v
}

func inferBrand(name string) string {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "jordan"):
		return "Air Jordan"
	case strings.Contains(n, "nike"), strings.Contains(n, "dunk"),
		strings.Contains(n, "air max"), strings.Contains(n, "air force"),
		strings.Contains(n, "zoom"), strings.Contains(n, "pegasus"):
		return "Nike"
	case strings.Contains(n, "adidas"), strings.Contains(n, "yeezy"),
		strings.Contains(n, "samba"), strings.Contains(n, "gazelle"):
		return "Adidas"
	case strings.Contains(n, "new balance"):
		return "New Balance"
	case strings.Contains(n, "asics"), strings.Contains(n, "gel-"):
		return "ASICS"
	case strings.Contains(n, "converse"), strings.Contains(n, "chuck taylor"):
		return "Converse"
	case strings.Contains(n, "puma"):
		return "Puma"
	case strings.Contains(n, "reebok"):
		return "Reebok"
	case strings.Contains(n, "vans"), strings.Contains(n, "old skool"):
		return "Vans"
	case strings.Contains(n, "new balance"):
		return "New Balance"
	case strings.Contains(n, "hoka"):
		return "Hoka"
	case strings.Contains(n, "salomon"):
		return "Salomon"
	case strings.Contains(n, "saucony"):
		return "Saucony"
	default:
		return "Unknown"
	}
}

func inWindow(dateStr string, days int) bool {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return false
	}
	now := time.Now().Truncate(24 * time.Hour)
	end := now.AddDate(0, 0, days)
	return !t.Before(now) && t.Before(end)
}

func scrape(doc *goquery.Document, days int) []Release {
	year := time.Now().Year()
	seen := map[string]bool{}
	var results []Release

	doc.Find("a[href]").Each(func(_ int, a *goquery.Selection) {
		text := strings.TrimSpace(a.Text())
		// Collapse whitespace
		spaceRE := regexp.MustCompile(`\s+`)
		text = spaceRE.ReplaceAllString(text, " ")

		m := anchorRE.FindStringSubmatch(text)
		if m == nil {
			return
		}

		dateStr, ok := parseDate(m[1], m[2], year)
		if !ok {
			return
		}
		if !inWindow(dateStr, days) {
			return
		}

		title := strings.TrimSpace(m[3])
		if len(title) < 6 {
			return
		}

		// Labeled price from parent element context (≤400 chars)
		parentText := a.Parent().Text()
		if len(parentText) > 400 {
			parentText = parentText[:400]
		}
		retail := extractLabeledPrice(parentText)

		href, _ := a.Attr("href")
		if strings.HasPrefix(href, "/") {
			href = "https://www.kicksonfire.com" + href
		}

		key := dateStr + "|" + strings.ToLower(title)
		if seen[key] {
			return
		}
		seen[key] = true

		results = append(results, Release{
			ReleaseDate:          dateStr,
			ShoeName:             title,
			Brand:                inferBrand(title),
			RetailPrice:          retail,
			EstimatedMarketValue: nil,
			ImageURL:             nil,
			SourcePrimary:        sourceName,
			SourceSecondary:      sourceURL,
			SourceURL:            sourceURL,
			ReleaseURL:           href,
		})
	})

	return results
}

func main() {
	days := flag.Int("days", 35, "Release window in days from today")
	output := flag.String("output", "", "Output file path (default: stdout)")
	flag.Parse()

	start := time.Now()

	doc, err := fetchHTML(sourceURL)
	if err != nil {
		log.Fatalf("fetch failed: %v", err)
	}

	releases := scrape(doc, *days)

	data, err := json.MarshalIndent(releases, "", "  ")
	if err != nil {
		log.Fatalf("json marshal: %v", err)
	}

	if *output == "" {
		fmt.Println(string(data))
	} else {
		if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
			log.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(*output, data, 0644); err != nil {
			log.Fatalf("write: %v", err)
		}
		fmt.Printf("%s saved: %d releases in %s\n", sourceName, len(releases), time.Since(start).Round(time.Millisecond))
	}
}
