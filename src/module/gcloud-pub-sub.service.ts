import { Injectable, Logger } from '@nestjs/common'
import { PubSub } from '@google-cloud/pubsub'
import { GoogleAuthOptions } from '../interfaces/gcloud-pub-sub.interface'
import { PublishOptions } from '@google-cloud/pubsub/build/src/topic'

@Injectable()
export class GcloudPubSubService {
	gcloudPubSubLib: PubSub

	/* istanbul ignore next */
	constructor(
		private readonly googleAuthOptions: GoogleAuthOptions,
		private readonly publishOptions: PublishOptions,
		private readonly logger = new Logger(GcloudPubSubService.name)
	) {
		this.gcloudPubSubLib = new PubSub(googleAuthOptions)
	}

	public publishMessage(
		topic: string,
		data: string,
		attributes: { [key: string]: string } = {}
	): Promise<string> {
		const dataBuffer = Buffer.from(data)
		this.logger.debug(`PubSub message sent to topic: ${topic}`)
		this.logger.debug(data)
		return this.gcloudPubSubLib.topic(topic, this.publishOptions).publish(dataBuffer, attributes)
	}
}
