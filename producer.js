const {Kafka} = require("kafkajs")

const args = process.argv.slice(2)
const brokers = [args[0] + ":9092"]
const topic = args[1]

const clientId = "nodejs-producer"
const kafka = new Kafka({clientId, brokers})
const producer = kafka.producer()

const produce = async () => {
    await producer.connect()
    let i = 0

    setInterval(async () => {
        try {
            await producer.send({
                topic,
                messages: [
                    {
                        key: String(i),
                        value: "this is message " + i,
                    },
                ],
            })

            console.log("writes: ", i)
            i++
        } catch (err) {
            console.error("could not write message " + err)
        }
    }, 1000)
}

produce().catch((err) => {
    console.error("error in producer: ", err)
})
