// reference: https://github.com/vladimirvivien/k8s-client-examples/blob/master/go/pvcwatch-ctl/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"

	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var ns, label, field, maxClaims string
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	flag.StringVar(&ns, "namespace", "default", "namespace")
	flag.StringVar(&label, "l", "", "Label selector")
	flag.StringVar(&field, "f", "", "Field selector")
	flag.StringVar(&maxClaims, "max-claims", "200Gi", "Maximum total claims to watch")
	flag.StringVar(&kubeconfig, "kubeconfig", kubeconfig, "kubeconfig file")
	flag.Parse()

	log.Println("Using kubeconfig: ", kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	ctrl := NewCtrl(clientset, ns, maxClaims)
	go ctrl.Run(stopCh)

	select {}
}

// Controller defines a controller
type Controller struct {
	factory informers.SharedInformerFactory
	lister  corelisters.PersistentVolumeClaimLister
	synced  cache.InformerSynced

	totalClaimedQuant resource.Quantity
	maxClaimedQuant   resource.Quantity
}

func NewCtrl(clientset kubernetes.Interface, namespace, maxClaimedQuant string) *Controller {
	informerFactory := informers.NewFilteredSharedInformerFactory(
		clientset,
		time.Second*3,
		namespace, nil,
	)
	informer := informerFactory.Core().V1().PersistentVolumeClaims()

	ctrl := &Controller{
		factory:         informerFactory,
		maxClaimedQuant: resource.MustParse(maxClaimedQuant),
	}

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.add,
		DeleteFunc: ctrl.delete,
		UpdateFunc: func(old, new interface{}) {
			newOne := new.(*coreV1.PersistentVolumeClaim)
			oldOne := old.(*coreV1.PersistentVolumeClaim)
			if newOne.ResourceVersion == oldOne.ResourceVersion {
				return
			}
			ctrl.update(new)
		},
	})
	ctrl.lister = informer.Lister()
	ctrl.synced = informer.Informer().HasSynced
	return ctrl
}

func (c *Controller) list() error {
	pvcs, err := c.lister.List(labels.Everything())
	if err != nil {
		return err
	}
	template := "%-32s%-8s%-8s\n"
	fmt.Println("--- PVCs ----")
	fmt.Printf(template, "NAME", "STATUS", "CAPACITY")
	var cap resource.Quantity
	for _, pvc := range pvcs {
		quant := pvc.Spec.Resources.Requests[coreV1.ResourceStorage]
		cap.Add(quant)
		fmt.Printf(template, pvc.Name, string(pvc.Status.Phase), quant.String())
	}

	fmt.Println("-----------------------------")
	fmt.Printf("Total capacity claimed: %s\n", cap.String())
	fmt.Println("-----------------------------")

	return nil
}

func (c *Controller) add(obj interface{}) {
	pvc, ok := obj.(*coreV1.PersistentVolumeClaim)
	if !ok {
		log.Println("unexpected type for object")
		return
	}
	quant := pvc.Spec.Resources.Requests[coreV1.ResourceStorage]
	c.totalClaimedQuant.Add(quant)
	log.Printf("ADD: PVC %s added, claim size %s\n", pvc.Name, quant.String())

	// is claim overage?
	if c.totalClaimedQuant.Cmp(c.maxClaimedQuant) == 1 {
		log.Printf("\nClaim overage reached: max %s at %s",
			c.maxClaimedQuant.String(),
			c.totalClaimedQuant.String(),
		)
		// trigger action
		log.Println("*** Taking action ***")
	}

}

func (c *Controller) update(obj interface{}) {
	pvc, ok := obj.(*coreV1.PersistentVolumeClaim)
	if !ok {
		log.Println("unexpected type for object")
		return
	}
	quant := pvc.Spec.Resources.Requests[coreV1.ResourceStorage]
	log.Printf("UPDATE: PVC %s updated, claim size %s\n", pvc.Name, quant.String())
}

func (c *Controller) delete(obj interface{}) {
	pvc, ok := obj.(*coreV1.PersistentVolumeClaim)
	if !ok {
		log.Println("unexpected type for object")
		return
	}
	quant := pvc.Spec.Resources.Requests[coreV1.ResourceStorage]
	c.totalClaimedQuant.Sub(quant)
	log.Printf("DELETED: PVC %s removed, claim size %s\n", pvc.Name, quant.String())

	if c.totalClaimedQuant.Cmp(c.maxClaimedQuant) <= 0 {
		log.Printf("Claim usage normal: max %s at %s",
			c.maxClaimedQuant.String(),
			c.totalClaimedQuant.String(),
		)
		// trigger action
		log.Println("*** Taking action ***")
	}
}

// Run implements the controller logic
func (c *Controller) Run(stopCh chan struct{}) {
	defer runtime.HandleCrash()

	log.Println("starting controller")
	defer log.Println("shutting down controller ")

	c.factory.Start(stopCh)

	if ok := cache.WaitForCacheSync(stopCh, c.synced); !ok {
		log.Println("failed to wait for caches to sync")
		return
	}

	if err := c.list(); err != nil {
		log.Println(err)
		return
	}
	<-stopCh
}
