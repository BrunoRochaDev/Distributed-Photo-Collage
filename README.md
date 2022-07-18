# Distributed Photo Collage
## Relatório do Trabalho de Melhoria
Bruno Rocha Moura — 97151

## Introdução

O presente relatório tem como objetivo descrever os procedimentos utilizados para a resolução do trabalho de melhoria de Computação Distribuída do segundo ano da Licenciatura em Engenharia Informática pela Universidade de Aveiro.

Como motivação, nos foi proposta a implementação de um sistema distribuído que deve receber um lista de imagens, redimensiona-las à uma altura comum para então uni-las horizontalmente tal que o resultado final é uma única imagem em tira.

A arquitetura do sistema deve ser baseada no algoritmo Map-Reduce, onde um processo Broker delega tarefas à processos Worker de forma que o trabalho computacional é distribuído entre os diferentes nós.

#IMAGEM DO RELATORIO AQUI

<p align="center">
Fig1. - Ilustração fornecida pelo guião de motivação
</p>


A motivação também requeria que o sistema fosse desenhado tendo em mente que os computadores onde os processos Worker seriam executados são lentos e pouco fiáveis. Em função disso, o sistema deve ter uma alta tolerência e recuperação de falhas.

Por último, mais uma restrição é que a comunicação entre os processos deve ser feita através de mensagens seguindo um protocolo à ser implementado por nós. Essas mensagens devem ser trocadas via sockets UDP.

## Problemas Iniciais

A maioria das complicações no desenvolvimento do sistema foram devido ao uso do protocolo de comunicação UDP. Por ser um protocolo *Fire-and-Forget*, não há garantias que mensagens enviadas chegarão no seu destino ou em que ordem.

Isso por si só já gera problemas quanto à atribuição de tarefas. Por exemplo, a situação onde um Broker atribui uma tarefa à um Worker, mas a mensagem é perdida. Na ausência de algum mecanismo de controle, o Broker não atribuíria essa tarefa para nenhum outro Worker (pois ele acredita que já tem alguém à realizando) e o Worker não só não a realizaria (pois não recebeu a mensagem) como também não receberia nenhuma outra (o Broker não delega tarefas para Workers com tarefas pendentes). Dessa forma, a tarefa jamais seria feita e o Worker permaneceria ociosos dali em diante.

O problema se agrava em combinação com outra restrição do UDP — seu limite relativamente baixo para tamanho de pacotes (65.507 bytes). As imagens comunicadas entre os processos podem facilmente exceder esse limite, principalmente durante os estágios finais onde as imagens são unidas. Se a comunicação fosse feita via TCP, uma solução fácil seria quebrar a mensagem em pacotes menores e envia-los sequencialmente para serem posteriormente reconstruídas pelo recessor. Infelizmente, entretanto, essa solução não se aplica ao nosso contexto devida à falta de garantia quanto a ordem de recessão de pacotes.

Questões de *networking* à parte, houve também um desafio quanto a encontrar uma estrutura de dados para representar as imagens. Essa estrutura deve conter as imagens originais assim como as dos processos intermediários de redimensionamento e união, de forma que é sempre possível perceber as suas relações entre si.

## Soluções

Para o problema da atribuição de tarefas, foi implementado um mecanismo de controle. Após o Broker atribuir uma tarefa à um Worker, o primeiro espera uma mensagem de confirmação de recessão da tarefa do segundo. Caso essa confirmação ocorra, a tarefa é atribuída oficialmente ao Worker, que certamente irá realiza-la exceto caso este morra (mais sobre isso mais à frente). Entretanto, se não houver uma mensagem de confirmação dentro de uma quantidade configurável de tempo, o Broker assume que o Worker não recebeu-la e tenta atribui-la de novo à um Worker (não necessariamente ao mesmo).

Uma alternativa para esta solução poderia ser o estabelecimento de um tempo limite para a realização de uma tarefa até se assumir que a mensagem foi perdida. Mas esta seria ineficiente, pois para qualquer dada operação o tempo limite seria ou longo demais (tempo perdido até atribuir a tarefa novamente) ou não suficiente, fazendo com que tarefas particularmente complicadas não pudessem ser realizadas por processos correndo em computadores mais lentos. O tempo limite também teria que ser configurado para cada lista de imagens e altura da imagem final.

Esse mecanismo de controle acabou por ser o maior *bottleneck* do sistema, mas este pode ser reduzido se o tempo limite para confirmação for diminuído o máximo possível.

Para contornar a limitação para o tamanho de pacotes, foi implementado um mecanismo para o pedido, envio e recessão de imagens. O conteúdo de cada imagem do sistema é dividido em fragmentos, cada um dos quais tem tamanho inferior à um limite estabelecido. Quando um processo quer anunciar a existência de uma imagem, este envia uma mensagem de anúncio que contém o identificador da imagem e sua quantidade de fragmentos. O processo recessor então envia mensagens de pedido para um fragmento em específico que são correspondidas por mensagens de resposta até o processo recessor ter todos os fragmentos necessessários para a reconstrução da imagem. Como é possível que pacotes se percam, pode demorar mais de uma tentativa para conseguir todos os fragmentos.

#IMAGEM AQUI

<p align="center">
Fig2. - Ilustração do processo de resolução de fragmentos
</p>

A estrutura de dados para armazenamento de imagem é uma árvore binária, onde as folhas são as imagens inicias que são populadas primeiro. Para cada passo intermediário no processo do sistema, novas imagens são inseridas na árvore. O processo de união entre as imagens só acontece entre as mais superficiais no momento da operação que são vizinhas. O processo acaba quando a imagem raiz é criada.

#IMAGEM AQUI
<p align="center">
Fig3. - Ilustração da árvore de imagens durante o processo. Imagens elegíveis para união tem uma borda preta.
</p>
