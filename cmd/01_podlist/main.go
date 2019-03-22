package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var ns string
	flag.StringVar(&ns, "namespace", "", "namespace")
	flag.Parse()

	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	log.Println("using kubeconfig:", kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(config)

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	pods, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		log.Fatal(err)
	}
	for i, pod := range pods.Items {
		log.Println(i, ":", pod.Namespace, pod.Name)
	}

}
