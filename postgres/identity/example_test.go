package identityoutbox_test

import (
	"fmt"

	identitypostgres "github.com/faustbrian/go-identity/postgres"
	identityoutbox "github.com/faustbrian/go-transactional-outbox/postgres/identity"
)

func ExampleNewContributor() {
	contributor, err := identityoutbox.NewContributor()
	if err != nil {
		panic(err)
	}
	registry, err := identitypostgres.NewContributorRegistry([]identitypostgres.Contributor{contributor})
	if err != nil {
		panic(err)
	}
	for _, descriptor := range registry.Descriptors() {
		fmt.Println(descriptor.Owner, descriptor.ContractID, descriptor.Version)
	}
	// Output: outbox/postgres identity.lifecycle 1
}
