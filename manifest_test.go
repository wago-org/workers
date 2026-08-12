package workers_test

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/wago-org/wago"
	"github.com/wago-org/workers"
	workersregister "github.com/wago-org/workers/register"
)

type manifestAuthor struct {
	Name string `json:"name"`
}

type manifestPackage struct {
	Module      string            `json:"module"`
	Version     string            `json:"version"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Stability   wago.Stability    `json:"stability"`
	License     string            `json:"license"`
	Homepage    string            `json:"homepage"`
	Repository  string            `json:"repository"`
	Authors     []manifestAuthor  `json:"authors"`
	Engines     map[string]string `json:"engines"`
	Platforms   []string          `json:"platforms"`
}

func TestManifestMatchesCatalogMetadata(t *testing.T) {
	raw, err := os.ReadFile("wago.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Schema  string            `json:"$schema"`
		Package manifestPackage   `json:"package"`
		Plugins map[string]string `json:"plugins"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Schema != "https://wago.sh/v1/schema.json" {
		t.Fatalf("manifest schema = %q", manifest.Schema)
	}
	providers := workersregister.Providers()
	if len(providers) != 1 {
		t.Fatalf("catalog providers = %d, want 1", len(providers))
	}
	definition := workers.Definition()
	if !reflect.DeepEqual(providers[0].Definition, definition) {
		t.Fatalf("catalog definition drifted\ncatalog=%#v\ncanonical=%#v", providers[0].Definition, definition)
	}
	assertManifestMetadata(t, manifest.Package, definition)
	if len(manifest.Plugins) != 0 {
		t.Fatalf("leaf manifest dependencies = %v", manifest.Plugins)
	}
	assertProviderCatalogCurrent(t, "github.com/wago-org/workers/register", providers)
}

func assertProviderCatalogCurrent(t *testing.T, importPath string, providers []wago.PluginProvider) {
	t.Helper()
	want, err := wago.EncodeProviderCatalog(importPath, providers)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(wago.ProviderCatalogFile)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s is stale; run wago plugin catalog", wago.ProviderCatalogFile)
	}
	if _, err := wago.DecodeProviderCatalog(got); err != nil {
		t.Fatalf("%s: %v", wago.ProviderCatalogFile, err)
	}
}

func assertManifestMetadata(t *testing.T, manifest manifestPackage, definition wago.PluginDefinition) {
	t.Helper()
	authors := make([]string, len(manifest.Authors))
	for i := range manifest.Authors {
		authors[i] = manifest.Authors[i].Name
	}
	if manifest.Module != definition.ID ||
		manifest.Version != definition.Version ||
		manifest.Name != definition.Name ||
		manifest.Description != definition.Description ||
		manifest.Stability != definition.Stability ||
		manifest.License != definition.Provenance.License ||
		manifest.Homepage != definition.Provenance.Homepage ||
		manifest.Repository != definition.Provenance.Repository ||
		!reflect.DeepEqual(authors, definition.Provenance.Authors) ||
		!reflect.DeepEqual(manifest.Engines, definition.Compatibility.Engines) ||
		!reflect.DeepEqual(manifest.Platforms, definition.Compatibility.Platforms) {
		t.Fatalf("manifest metadata drifted\nmanifest=%#v\ndefinition=%#v", manifest, definition)
	}
}
