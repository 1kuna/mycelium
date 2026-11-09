package myce

func breadthRecipe(platforms ...string) EnvironmentRecipe {
	data := "version: 6\nenvironments:\n  selected:\n    packages:\n"
	for _, platform := range platforms {
		data += "      " + platform + ": []\n"
	}
	data += "  unused:\n    packages:\n      win-64: []\n"
	r := EnvironmentRecipe{Backend: "pixi", Environment: "selected", LockFile: "pixi.lock", Files: map[string][]byte{"pixi.lock": []byte(data), "pixi.toml": []byte("[workspace]\n")}}
	r.Key = identityHash(r)
	return r
}
