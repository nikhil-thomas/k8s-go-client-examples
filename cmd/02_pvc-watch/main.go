package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/watch"

	"k8s.io/client-go/kubernetes"

	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "k8s.io/api/core/v1"
)

func main() {
	var ns, label, field, maxClaims string
	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	flag.StringVar(&ns, "namespace", "", "namespace")
	flag.StringVar(&label, "l", "", "label selector")
	flag.StringVar(&field, "f", "", "field selector")
	flag.StringVar(&maxClaims, "max-claims", "10Mi", "maximum total claims")
	flag.StringVar(&kubeconfig, "kubeconfig", kubeconfig, "kubeconfig file")
	flag.Parse()

	var totalClaimedQuant resource.Quantity
	maxClaimedQuant := resource.MustParse(maxClaims)

	fmt.Println("using kubeconfig", kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalln(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln(err)
	}
	api := clientset.CoreV1()

	listOptions := metav1.ListOptions{
		LabelSelector: label,
		FieldSelector: field,
	}
	pvcs, err := api.PersistentVolumeClaims(ns).List(listOptions)
	if err != nil {
		log.Fatalln(err)
	}
	printPVCs(pvcs)

	watcher, err := clientset.CoreV1().PersistentVolumeClaims(ns).Watch(listOptions)
	if err != nil {
		log.Fatalln(err)
	}
	ch := watcher.ResultChan()
	fmt.Printf("--pvc watch (max claim %v)\n", maxClaimedQuant.String())

	for event := range ch {
		pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			log.Fatal("unexpected type")
		}
		quant := pvc.Spec.Resources.Requests[v1.ResourceStorage]

		switch event.Type {
		case watch.Added:
			totalClaimedQuant.Add(quant)
			log.Printf("PVC %s added, claim size %s\n", pvc.Name, quant.String())

			if totalClaimedQuant.Cmp(maxClaimedQuant) == 1 {
				log.Printf("\nclaim overage reached: max %s at %s",
					maxClaimedQuant.String(),
					totalClaimedQuant.String(),
				)
				log.Println("taking action")
			} else {
				log.Printf("Claim usage normal: max %s at %s",
					maxClaimedQuant.String(),
					totalClaimedQuant.String(),
				)
			}
		case watch.Modified:
			log.Printf("pvc %s modified", pvc.Name)
		case watch.Deleted:
			quant := pvc.Spec.Resources.Requests[v1.ResourceStorage]
			totalClaimedQuant.Sub(quant)
			log.Printf("PVC %s removed, size %s \n", pvc.Name, quant.String())

			if totalClaimedQuant.Cmp(maxClaimedQuant) <= 0 {
				log.Printf("Claim usage normal: max %s at %s",
					maxClaimedQuant.String(),
					totalClaimedQuant.String(),
				)
			}
		case watch.Error:
			log.Printf("pvc watch error")
		}
		log.Printf("\nAt %3.1f%% claim capacity (%s/%s)\n",
			float64(totalClaimedQuant.Value())/float64(maxClaimedQuant.Value())*100,
			totalClaimedQuant.String(),
			maxClaimedQuant.String(),
		)
	}

}

func printPVCs(pvcs *v1.PersistentVolumeClaimList) {
	if len(pvcs.Items) == 0 {
		log.Println("no PVCs found")
		return
	}
	template := "%-32s%-8s%-8s\n"
	var cap resource.Quantity
	fmt.Printf(template, "NAME", "STATUS", "CAPACITY")
	for _, pvc := range pvcs.Items {
		quant := pvc.Spec.Resources.Requests[v1.ResourceStorage]
		cap.Add(quant)
		fmt.Printf(template, pvc.Name, string(pvc.Status.Phase), quant.String())
	}

	fmt.Println("::::::::::--------------------::::::::::")
	fmt.Println("Total capacity claimed: %s\n", cap.String())
	fmt.Println("::::::::::--------------------::::::::::")
}
